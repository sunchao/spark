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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{Alias, EqualNullSafe, Expression, If, Literal, Not, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, Filter, LogicalPlan, Project, ReplaceData, Union, UpdateTable, WriteDelta}
import org.apache.spark.sql.catalyst.util.RowDeltaUtils._
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.connector.write.{RowLevelOperationTable, SupportsDelta}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.UPDATE
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.BooleanType

/**
 * Assigns a rewrite plan for v2 tables that support rewriting data to handle UPDATE statements.
 *
 * This rule assumes the commands have been fully resolved and all assignments have been aligned.
 * That's why it must be run after [[AlignRowLevelCommandAssignments]].
 *
 * This rule also must be run in the same batch with [[DeduplicateRelations]].
 */
object RewriteUpdateTable extends RewriteRowLevelCommand {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u @ UpdateTable(aliasedTable, assignments, cond, None)
        if u.resolved && isIcebergTable(aliasedTable) =>

      EliminateSubqueryAliases(aliasedTable) match {
        case r @ DataSourceV2Relation(tbl: SupportsRowLevelOperations, _, _, _, _) =>
          val operation = buildRowLevelOperation(tbl, UPDATE)
          val table = RowLevelOperationTable(tbl, operation)
          val rewritePlan = operation match {
            case _: SupportsDelta =>
              buildWriteDeltaPlan(r, table, assignments, cond)
            case _ if cond.exists(SubqueryExpression.hasSubquery) =>
              buildReplaceDataWithUnionPlan(r, table, assignments, cond)
            case _ =>
              buildReplaceDataPlan(r, table, assignments, cond)
          }
          UpdateTable(r, assignments, cond, Some(rewritePlan))

        case _ =>
          u
      }
  }

  // build a rewrite plan for sources that support replacing groups of data (e.g. files, partitions)
  // if the condition does NOT contain a subquery
  private def buildReplaceDataPlan(
      relation: DataSourceV2Relation,
      table: RowLevelOperationTable,
      assignments: Seq[Assignment],
      cond: Option[Expression]): ReplaceData = {

    // resolve all needed attrs (e.g. metadata attrs for grouping data on write)
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, table.operation)

    // construct a scan relation and include all required metadata columns
    val scanAttrs = dedupAttrs(relation.output ++ metadataAttrs)
    val scanRelation = relation.copy(table = table, output = scanAttrs)

    // build a plan with updated rows
    val updateCond = cond.getOrElse(Literal.TrueLiteral)
    val allRowsPlan = buildUpdateProjection(scanRelation, assignments, updateCond)

    // build a plan to replace read groups in the table
    val writeRelation = relation.copy(table = table)
    ReplaceData(writeRelation, allRowsPlan, relation)
  }

  // build a rewrite plan for sources that support replacing groups of data (e.g. files, partitions)
  // if the condition contains a subquery
  private def buildReplaceDataWithUnionPlan(
      relation: DataSourceV2Relation,
      table: RowLevelOperationTable,
      assignments: Seq[Assignment],
      cond: Option[Expression]): ReplaceData = {

    // resolve all needed attrs (e.g. metadata attrs for grouping data on write)
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, table.operation)

    // assume DeduplicateRelations will take care of duplicated attr IDs
    val scanAttrs = dedupAttrs(relation.output ++ metadataAttrs)
    val scanRelation = relation.copy(table = table, output = scanAttrs)

    // build a plan for records that match the cond and should be updated
    val updateCond = cond.getOrElse(Literal.TrueLiteral)
    val matchedRowsPlan = Filter(updateCond, scanRelation)
    val updatedRowsPlan = buildUpdateProjection(matchedRowsPlan, assignments)

    // build a plan for records that did not match the cond but had to be copied over
    val remainingRowFilter = Not(EqualNullSafe(updateCond, Literal(true, BooleanType)))
    val remainingRowsPlan = Filter(remainingRowFilter, scanRelation)

    // new state is a union of updated and copied over records
    val allRowsPlan = Union(updatedRowsPlan, remainingRowsPlan)

    // build a plan to replace read groups in the table
    val writeRelation = relation.copy(table = table)
    ReplaceData(writeRelation, allRowsPlan, relation)
  }

  // build a rewrite plan for sources that support row deltas
  private def buildWriteDeltaPlan(
      relation: DataSourceV2Relation,
      table: RowLevelOperationTable,
      assignments: Seq[Assignment],
      cond: Option[Expression]): WriteDelta = {

    // resolve all needed attrs (e.g. row ID and any required metadata attrs)
    val rowAttrs = relation.output
    val rowIdAttrs = resolveRowIdAttrs(relation, table.operation)
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, table.operation)

    // construct a scan relation and include all required metadata columns
    val scanAttrs = dedupAttrs(rowAttrs ++ rowIdAttrs ++ metadataAttrs)
    val scanRelation = relation.copy(table = table, output = scanAttrs)

    // build a plan for updated records that match the cond
    val updateCond = cond.getOrElse(Literal.TrueLiteral)
    val matchedRowsPlan = Filter(updateCond, scanRelation)
    val updatedRowsPlan = buildUpdateProjection(matchedRowsPlan, assignments)
    val operationType = Alias(Literal(UPDATE_OPERATION), OPERATION_COLUMN)()
    val project = Project(operationType +: updatedRowsPlan.output, updatedRowsPlan)

    // build a plan to write the row delta to the table
    val writeRelation = relation.copy(table = table)
    val projections = buildWriteDeltaProjections(project, rowAttrs, rowIdAttrs, metadataAttrs)
    WriteDelta(writeRelation, project, relation, projections)
  }

  // this method assumes the assignments have been already aligned before
  // the condition passed to this method may be different from the operation condition
  private def buildUpdateProjection(
      plan: LogicalPlan,
      assignments: Seq[Assignment],
      cond: Expression = Literal.TrueLiteral): LogicalPlan = {

    // TODO: avoid executing the condition for each column
    // TODO: validate we cannot modify the row id column

    // the plan output may include metadata columns that are not modified
    // that's why the number of assignments may not match the number of plan output columns

    val assignedValues = assignments.map(_.value)
    val updatedValues = plan.output.zipWithIndex.map { case (attr, index) =>
      if (index < assignments.size) {
        val assignedExpr = assignedValues(index)
        val updatedValue = If(cond, assignedExpr, attr)
        Alias(updatedValue, attr.name)()
      } else {
        attr
      }
    }

    Project(updatedValues, plan)
  }
}
