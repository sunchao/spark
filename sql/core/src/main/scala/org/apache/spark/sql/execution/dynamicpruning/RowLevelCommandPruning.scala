/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.dynamicpruning

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeMap, AttributeReference, DynamicPruningSubquery, Expression, Literal, PredicateHelper, V2ExpressionUtils}
import org.apache.spark.sql.catalyst.planning.RewrittenRowLevelCommand
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, JoinHint, LogicalPlan, MergeIntoTable, Project, ReplaceData, RowLevelCommand, UpdateTable}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.read.SupportsRuntimeFiltering
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

/**
 * A rule that adds a runtime filter for row-level commands.
 *
 * Note that only group-based rewrite plans (i.e. [[ReplaceData]]) are taken into account.
 * Row-based rewrite plans are subject to usual runtime filtering.
 */
case class RowLevelCommandPruning(
    optimizeSubqueriesRule: Rule[LogicalPlan]) extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    // apply special dynamic filtering only for plans that don't support deltas
    case RewrittenRowLevelCommand(
        command: RowLevelCommand,
        DataSourceV2ScanRelation(_, scan: SupportsRuntimeFiltering, _),
        rewritePlan: ReplaceData) if conf.dynamicPartitionPruningEnabled =>

      // use reference equality to find exactly the required scan relations
      val newRewritePlan = rewritePlan transformUp {
        case r: DataSourceV2ScanRelation if r.scan eq scan =>
          val pruningKeys = V2ExpressionUtils.resolveRefs[Attribute](scan.filterAttributes, r)
          val dynamicPruningCond = buildDynamicPruningCondition(r, command, pruningKeys)
          val filter = Filter(dynamicPruningCond, r)
          // always optimize dynamic filtering subqueries for row-level commands as it is important
          // to rewrite introduced predicates as joins because Spark recently stopped optimizing
          // dynamic subqueries to facilitate broadcast reuse
          optimizeSubqueriesRule(filter)
      }
      command.withNewRewritePlan(newRewritePlan)
  }

  private def buildDynamicPruningCondition(
      relation: DataSourceV2ScanRelation,
      command: RowLevelCommand,
      pruningKeys: Seq[Attribute]): Expression = {

    // construct a filtering plan with the original scan relation
    val cond = command.condition.getOrElse(Literal.TrueLiteral)
    val matchingRowsPlan = command match {
      case m: MergeIntoTable =>
        Join(relation, m.sourceTable, LeftSemi, Some(cond), JoinHint.NONE)

      case u: UpdateTable =>
        // UPDATEs with subqueries may be rewritten using a UNION with two identical scan relations
        // each scan relation will get its own dynamic filter that will be shared during execution
        // the analyzer will assign different expr IDs for each scan relation output attributes
        // that's why the condition may refer to invalid attr expr IDs and must be transformed
        val attrMap = AttributeMap(u.table.output.zip(relation.output))
        val transformedCond = cond transform {
          case attr: AttributeReference if attrMap.contains(attr) => attrMap(attr)
        }
        Filter(transformedCond, relation)

      case _ =>
        Filter(cond, relation)
    }

    // clone the original relation in the filtering plan and assign new expr IDs to avoid conflicts
    val transformedMatchingRowsPlan = matchingRowsPlan transformUpWithNewOutput {
      case r: DataSourceV2ScanRelation if r eq relation =>
        val oldOutput = r.output
        val newOutput = oldOutput.map(_.newInstance())
        r.copy(output = newOutput) -> oldOutput.zip(newOutput)
    }

    val filterableScan = relation.scan.asInstanceOf[SupportsRuntimeFiltering]
    val buildKeys = V2ExpressionUtils.resolveRefs[Attribute](
      filterableScan.filterAttributes,
      transformedMatchingRowsPlan)
    val buildQuery = Project(buildKeys, transformedMatchingRowsPlan)
    val dynamicPruningSubqueries = pruningKeys.zipWithIndex.map { case (key, index) =>
      DynamicPruningSubquery(key, buildQuery, buildKeys, index, onlyInBroadcast = false)
    }

    // combine all dynamic subqueries to produce the final condition
    dynamicPruningSubqueries.reduce(And)
  }
}
