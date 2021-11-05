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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.analysis.{FieldName, NamedRelation}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, Expression}
import org.apache.spark.sql.connector.catalog.SupportsOptimize
import org.apache.spark.sql.connector.expressions.{SortOrder => V2SortOrder}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

case class OptimizeTable(
    table: NamedRelation,
    predicate: Expression,
    strategy: OptimizeStrategy,
    options: Map[String, String]) extends LeafCommand {

  override lazy val resolved: Boolean = table.resolved && predicate.resolved

  private lazy val resolvedOutput = table match {
    case DataSourceV2Relation(t: SupportsOptimize, _, _, _, _) =>
      t.optimizeOutput.toAttributes
    case _ =>
      Seq.empty[AttributeReference]
  }

  override def inputSet: AttributeSet = AttributeSet(table.output)

  override def output: Seq[Attribute] = if (resolved) resolvedOutput else Seq.empty
}

sealed trait OptimizeStrategy

case object BinPack extends OptimizeStrategy

case class OrderBy(ordering: Seq[V2SortOrder]) extends OptimizeStrategy

case class ZOrder(columns: Seq[FieldName]) extends OptimizeStrategy {
  private[sql] def colNames: Seq[Array[String]] = {
    columns.foreach { col =>
      require(col.resolved, s"FieldName $col should be resolved.")
    }
    columns.map(_.name.toArray)
  }
}
