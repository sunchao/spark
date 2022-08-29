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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, Expression, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.RewrittenRowLevelCommand
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources.Filter

object RowLevelCommandScanRelationPushDown extends Rule[LogicalPlan] with PredicateHelper {
  import DataSourceV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    // push down the filter from the command condition instead of the filter in the rewrite plan,
    // which may be negated for copy-on-write operations
    case RewrittenRowLevelCommand(command, relation: DataSourceV2Relation, rewritePlan) =>
      val table = relation.table.asRowLevelOperationTable
      val scanBuilder = table.newScanBuilder(relation.options)

      val (pushedFilters, remainingFilters) = command.condition match {
        case Some(cond) => pushFilters(cond, scanBuilder, relation.output)
        case None => (Nil, Nil)
      }

      val (scan, output) = PushDownUtils.pruneColumns(scanBuilder, relation, relation.output, Nil)

      logInfo(
        s"""
           |Pushing operators to ${relation.name}
           |Pushed filters: ${pushedFilters.mkString(", ")}
           |Filters that were not pushed: ${remainingFilters.mkString(",")}
           |Output: ${output.mkString(", ")}
         """.stripMargin)

      // replace DataSourceV2Relation with DataSourceV2ScanRelation for the row operation table
      // there may be multiple scan relations for UPDATEs that rely on the UNION approach
      val newRewritePlan = rewritePlan transform {
        case r: DataSourceV2Relation if r.table eq table =>
          DataSourceV2ScanRelation(r, scan, PushDownUtils.toOutputAttrs(scan.readSchema(), r))
      }

      command.withNewRewritePlan(newRewritePlan)
  }

  private def pushFilters(
      cond: Expression,
      scanBuilder: ScanBuilder,
      tableAttrs: Seq[AttributeReference]): (Seq[Filter], Seq[Expression]) = {

    val tableAttrSet = AttributeSet(tableAttrs)
    val filters = splitConjunctivePredicates(cond).filter(_.references.subsetOf(tableAttrSet))
    val normalizedFilters = DataSourceStrategy.normalizeExprs(filters, tableAttrs)
    val (_, normalizedFiltersWithoutSubquery) =
      normalizedFilters.partition(SubqueryExpression.hasSubquery)

    PushDownUtils.pushFilters(scanBuilder, normalizedFiltersWithoutSubquery)
  }
}
