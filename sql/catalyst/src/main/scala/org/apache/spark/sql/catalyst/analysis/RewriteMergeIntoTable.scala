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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, Expression, IsNotNull, Literal, MonotonicallyIncreasingID, SubqueryExpression, V2ExpressionUtils}
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftAnti, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, DeleteAction, Filter, HintInfo, InsertAction, Join, JoinHint, LogicalPlan, MergeAction, MergeIntoTable, MergeRows, MergeRowsParams, NO_BROADCAST_HASH, Project, ReplaceData, UpdateAction, WriteDelta}
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.{DELETE_OPERATION, INSERT_OPERATION, OPERATION_COLUMN, UPDATE_OPERATION}
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.connector.write.{RowLevelOperationTable, SupportsDelta}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.MERGE
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.IntegerType

/**
 * Assigns a rewrite plan for v2 tables that support rewriting data to handle MERGE statements.
 *
 * This rule assumes the commands have been fully resolved and all assignments have been aligned.
 * That's why it must be run after [[AlignRowLevelCommandAssignments]].
 */
object RewriteMergeIntoTable extends RewriteRowLevelCommand {

  private final val ROW_FROM_SOURCE = "__row_from_source"
  private final val ROW_FROM_TARGET = "__row_from_target"
  private final val ROW_ID = "__row_id"

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case m @ MergeIntoTable(aliasedTable, source, cond, matchedActions, notMatchedActions, None)
        if m.resolved && isIcebergTable(aliasedTable) && matchedActions.isEmpty &&
           notMatchedActions.size == 1 =>

      validateMergeIntoConditions(m)

      EliminateSubqueryAliases(aliasedTable) match {
        case r: DataSourceV2Relation =>
          // NOT MATCHED conditions may only refer to columns in source so we can push them down
          val insertAction = notMatchedActions.head.asInstanceOf[InsertAction]
          val filteredSource = insertAction.condition match {
            case Some(insertCond) => Filter(insertCond, source)
            case None => source
          }

          // when there are no MATCHED actions, use a left anti join to remove any matching rows
          // and switch to using a regular append instead of a row-level merge
          // only unmatched source rows that match the condition are appended to the table
          val joinPlan = Join(filteredSource, r, LeftAnti, Some(cond), JoinHint.NONE)

          val outputExprs = insertAction.assignments.map(_.value)
          val outputColNames = r.output.map(_.name)
          val outputCols = outputExprs.zip(outputColNames).map { case (expr, name) =>
            Alias(expr, name)()
          }
          val project = Project(outputCols, joinPlan)

          AppendData(r, project, Map.empty, isByName = false)

        case _ =>
          m
      }

    case m @ MergeIntoTable(aliasedTable, source, cond, matchedActions, notMatchedActions, None)
        if m.resolved && isIcebergTable(aliasedTable) && matchedActions.isEmpty =>

      validateMergeIntoConditions(m)

      EliminateSubqueryAliases(aliasedTable) match {
        case r: DataSourceV2Relation =>

          // when there are no MATCHED actions, use a left anti join to remove any matching rows
          // and switch to using a regular append instead of a row-level merge
          // only unmatched source rows that match action conditions are appended to the table
          val joinPlan = Join(source, r, LeftAnti, Some(cond), JoinHint.NONE)

          // we still have to merge rows as we have multiple not matched actions
          val mergeRowsParams = MergeRowsParams(
            isSourceRowPresent = TrueLiteral,
            isTargetRowPresent = FalseLiteral,
            matchedConditions = Nil,
            matchedOutputs = Nil,
            notMatchedConditions = notMatchedActions.map(actionCondition),
            notMatchedOutputs = notMatchedActions.map(output(_, Nil)),
            targetOutput = Nil,
            joinedAttributes = joinPlan.output,
            rowIdAttrs = Nil,
            performCardinalityCheck = false,
            emitNotMatchedTargetRows = false)
          val mergeRows = buildMergeRows(mergeRowsParams, r.output, joinPlan)

          AppendData(r, mergeRows, Map.empty, isByName = false)

        case _ =>
          m
      }

    case m @ MergeIntoTable(aliasedTable, source, cond, matchedActions, notMatchedActions, None)
        if m.resolved && isIcebergTable(aliasedTable) =>

      validateMergeIntoConditions(m)

      EliminateSubqueryAliases(aliasedTable) match {
        case r @ DataSourceV2Relation(tbl: SupportsRowLevelOperations, _, _, _, _) =>
          val operation = buildRowLevelOperation(tbl, MERGE)
          val table = RowLevelOperationTable(tbl, operation)
          val rewritePlan = operation match {
            case _: SupportsDelta =>
              buildWriteDeltaPlan(r, table, source, cond, matchedActions, notMatchedActions)
            case _ =>
              buildReplaceDataPlan(r, table, source, cond, matchedActions, notMatchedActions)
          }

          m.copy(rewritePlan = Some(rewritePlan))

        case _ =>
          m
      }
  }

  // build a rewrite plan for sources that support replacing groups of data (e.g. files, partitions)
  private def buildReplaceDataPlan(
      relation: DataSourceV2Relation,
      table: RowLevelOperationTable,
      source: LogicalPlan,
      cond: Expression,
      matchedActions: Seq[MergeAction],
      notMatchedActions: Seq[MergeAction]): ReplaceData = {

    // resolve all needed attrs (e.g. metadata attrs for grouping data on write)
    val rowAttrs = relation.output
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, table.operation)

    // construct a scan relation and include all required metadata columns
    val scanAttrs = rowAttrs ++ metadataAttrs
    val scanRelation = relation.copy(table = table, output = scanAttrs)

    // project an extra column to check if a target row exists after the join
    // project a synthetic row ID so that we can perform the cardinality check
    val rowFromTarget = Alias(TrueLiteral, ROW_FROM_TARGET)()
    val rowId = Alias(MonotonicallyIncreasingID(), ROW_ID)()
    val targetTableProjExprs = scanRelation.output ++ Seq(rowFromTarget, rowId)
    val targetTableProj = Project(targetTableProjExprs, scanRelation)

    // project an extra column to check if a source row exists after the join
    val sourceTableProjExprs = source.output :+ Alias(TrueLiteral, ROW_FROM_SOURCE)()
    val sourceTableProj = Project(sourceTableProjExprs, source)

    // use left outer join if there is no NOT MATCHED action, unmatched source rows can be discarded
    // use full outer join in all other cases, unmatched source rows may be needed
    // disable broadcasts for the target table to perform the cardinality check
    val joinType = if (notMatchedActions.isEmpty) LeftOuter else FullOuter
    val joinHint = JoinHint(leftHint = Some(HintInfo(Some(NO_BROADCAST_HASH))), rightHint = None)
    val joinPlan = Join(targetTableProj, sourceTableProj, joinType, Some(cond), joinHint)

    val rowIdAttr = V2ExpressionUtils.resolveRef[AttributeReference](
      FieldReference(ROW_ID),
      joinPlan)
    val rowFromSourceAttr = V2ExpressionUtils.resolveRef[AttributeReference](
      FieldReference(ROW_FROM_SOURCE),
      joinPlan)
    val rowFromTargetAttr = V2ExpressionUtils.resolveRef[AttributeReference](
      FieldReference(ROW_FROM_TARGET),
      joinPlan)

    // add an extra matched action to output the original row if none of the actual actions matched
    // this is needed to keep target rows that should be copied over as we are working with groups
    val mergeRowsParams = MergeRowsParams(
      isSourceRowPresent = IsNotNull(rowFromSourceAttr),
      isTargetRowPresent = IsNotNull(rowFromTargetAttr),
      matchedConditions = matchedActions.map(actionCondition) :+ TrueLiteral,
      matchedOutputs = matchedActions.map(output(_, metadataAttrs)) :+ Some(scanAttrs),
      notMatchedConditions = notMatchedActions.map(actionCondition),
      notMatchedOutputs = notMatchedActions.map(output(_, metadataAttrs)),
      targetOutput = scanAttrs,
      joinedAttributes = joinPlan.output,
      rowIdAttrs = Seq(rowIdAttr),
      performCardinalityCheck = isCardinalityCheckNeeded(matchedActions),
      emitNotMatchedTargetRows = true)
    val mergeRows = buildMergeRows(mergeRowsParams, scanAttrs, joinPlan)

    // build a plan to replace read groups in the table
    val writeRelation = relation.copy(table = table)
    ReplaceData(writeRelation, mergeRows, relation)
  }

  // build a rewrite plan for sources that support row deltas
  private def buildWriteDeltaPlan(
      relation: DataSourceV2Relation,
      table: RowLevelOperationTable,
      source: LogicalPlan,
      cond: Expression,
      matchedActions: Seq[MergeAction],
      notMatchedActions: Seq[MergeAction]): WriteDelta = {

    // resolve all needed attrs (e.g. row ID and any required metadata attrs)
    val rowAttrs = relation.output
    val rowIdAttrs = resolveRowIdAttrs(relation, table.operation)
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, table.operation)

    // construct a scan relation and include all required metadata columns
    val scanAttrs = dedupAttrs(rowAttrs ++ rowIdAttrs ++ metadataAttrs)
    val scanRelation = relation.copy(table = table, output = scanAttrs)

    // project an extra column to check if a target row exists after the join
    val targetTableProjExprs = scanRelation.output :+ Alias(TrueLiteral, ROW_FROM_TARGET)()
    val targetTableProj = Project(targetTableProjExprs, scanRelation)

    // project an extra column to check if a source row exists after the join
    val sourceTableProjExprs = source.output :+ Alias(TrueLiteral, ROW_FROM_SOURCE)()
    val sourceTableProj = Project(sourceTableProjExprs, source)

    // use inner join if there is no NOT MATCHED action, unmatched source rows can be discarded
    // use right outer join in all other cases, unmatched source rows may be needed
    // also disable broadcasts for the target table to perform the cardinality check later
    val joinType = if (notMatchedActions.isEmpty) Inner else RightOuter
    val joinHint = JoinHint(leftHint = Some(HintInfo(Some(NO_BROADCAST_HASH))), rightHint = None)
    val joinPlan = Join(targetTableProj, sourceTableProj, joinType, Some(cond), joinHint)

    val rowFromSourceAttr = V2ExpressionUtils.resolveRef[AttributeReference](
      FieldReference(ROW_FROM_SOURCE),
      joinPlan)
    val rowFromTargetAttr = V2ExpressionUtils.resolveRef[AttributeReference](
      FieldReference(ROW_FROM_TARGET),
      joinPlan)
    val deleteRowValues = buildDeltaDeleteRowValues(rowAttrs, rowIdAttrs)
    val metadataScanAttrs = scanAttrs.filterNot(relation.outputSet.contains)

    val mergeRowsParams = MergeRowsParams(
      isSourceRowPresent = IsNotNull(rowFromSourceAttr),
      isTargetRowPresent = IsNotNull(rowFromTargetAttr),
      matchedConditions = matchedActions.map(actionCondition),
      matchedOutputs = matchedActions.map(deltaOutput(_, deleteRowValues, metadataScanAttrs)),
      notMatchedConditions = notMatchedActions.map(actionCondition),
      notMatchedOutputs = notMatchedActions.map(deltaOutput(_, deleteRowValues, metadataScanAttrs)),
      targetOutput = Nil,
      joinedAttributes = joinPlan.output,
      rowIdAttrs = rowIdAttrs,
      performCardinalityCheck = isCardinalityCheckNeeded(matchedActions),
      emitNotMatchedTargetRows = false)
    val operationTypeAttr = AttributeReference(OPERATION_COLUMN, IntegerType, nullable = false)()
    val mergeRows = buildMergeRows(mergeRowsParams, operationTypeAttr +: scanAttrs, joinPlan)

    // build a plan to write the row delta to the table
    val writeRelation = relation.copy(table = table)
    val projections = buildWriteDeltaProjections(mergeRows, rowAttrs, rowIdAttrs, metadataAttrs)
    WriteDelta(writeRelation, mergeRows, relation, projections)
  }

  private def actionCondition(action: MergeAction): Expression = {
    action.condition.getOrElse(TrueLiteral)
  }

  private def output(
      clause: MergeAction,
      metadataAttrs: Seq[Attribute]): Option[Seq[Expression]] = {

    clause match {
      case u: UpdateAction =>
        Some(u.assignments.map(_.value) ++ metadataAttrs)

      case _: DeleteAction =>
        None

      case i: InsertAction =>
        Some(i.assignments.map(_.value) ++ metadataAttrs.map(attr => Literal(null, attr.dataType)))

      case other =>
        throw new AnalysisException(s"Unexpected action: $other")
    }
  }

  private def deltaOutput(
      action: MergeAction,
      deleteRowValues: Seq[Expression],
      metadataAttrs: Seq[Attribute]): Option[Seq[Expression]] = {

    action match {
      case u: UpdateAction =>
        Some(Seq(Literal(UPDATE_OPERATION)) ++ u.assignments.map(_.value) ++ metadataAttrs)

      case _: DeleteAction =>
        Some(Seq(Literal(DELETE_OPERATION)) ++ deleteRowValues ++ metadataAttrs)

      case i: InsertAction =>
        val metadataAttrValues = metadataAttrs.map(attr => Literal(null, attr.dataType))
        Some(Seq(Literal(INSERT_OPERATION)) ++ i.assignments.map(_.value) ++ metadataAttrValues)

      case other =>
        throw new AnalysisException(s"Unexpected action: $other")
    }
  }

  private def buildMergeRows(
      params: MergeRowsParams,
      attrs: Seq[Attribute],
      joinPlan: LogicalPlan): MergeRows = {

    val outputs = params.matchedOutputs.flatten ++ params.notMatchedOutputs.flatten
    assert(outputs.nonEmpty, "must be at least one output")

    val nullabilityMap = attrs.indices.map { index =>
      index -> outputs.exists(output => output(index).nullable)
    }.toMap

    val output = attrs.zipWithIndex.map { case (attr, index) =>
      AttributeReference(attr.name, attr.dataType, nullabilityMap(index))()
    }

    MergeRows(params, output, joinPlan)
  }

  private def buildDeltaDeleteRowValues(
      rowAttrs: Seq[Attribute],
      rowIdAttrs: Seq[Attribute]): Seq[Expression] = {

    // nullify all row attrs that are not part of the row ID
    val rowIdAttSet = AttributeSet(rowIdAttrs)
    rowAttrs.map {
      case attr if rowIdAttSet.contains(attr) => attr
      case attr => Literal(null, attr.dataType)
    }
  }

  private def isCardinalityCheckNeeded(actions: Seq[MergeAction]): Boolean = actions match {
    case Seq(DeleteAction(None)) => false
    case _ => true
  }

  private def validateMergeIntoConditions(merge: MergeIntoTable): Unit = {
    checkMergeIntoCondition(merge.mergeCondition, "SEARCH")
    val actions = merge.matchedActions ++ merge.notMatchedActions
    actions.foreach {
      case DeleteAction(Some(cond)) => checkMergeIntoCondition(cond, "DELETE")
      case UpdateAction(Some(cond), _) => checkMergeIntoCondition(cond, "UPDATE")
      case InsertAction(Some(cond), _) => checkMergeIntoCondition(cond, "INSERT")
      case _ => // OK
    }
  }

  private def checkMergeIntoCondition(cond: Expression, condName: String): Unit = {
    if (!cond.deterministic) {
      throw new AnalysisException(
        s"Non-deterministic functions are not supported in $condName conditions of " +
        s"MERGE operations: $cond")
    }
    if (SubqueryExpression.hasSubquery(cond)) {
      throw new AnalysisException(
        s"Subqueries are not supported in conditions of MERGE operations. " +
        s"Found a subquery in the $condName condition: ${cond.sql}")
    }
    if (cond.find(_.isInstanceOf[AggregateExpression]).isDefined) {
      throw new AnalysisException(
        s"Agg functions are not supported in $condName conditions of MERGE operations: " + cond)
    }
  }
}
