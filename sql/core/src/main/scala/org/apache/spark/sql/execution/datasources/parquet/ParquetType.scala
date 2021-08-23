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

package org.apache.spark.sql.execution.datasources.parquet

import scala.collection.mutable
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.io.{GroupColumnIO, ColumnIOUtil, PrimitiveColumnIO}
import org.apache.parquet.schema.Type.Repetition

import org.apache.spark.sql.types.DataType

/**
 * Rich type information for a Parquet type together with its SparkSQL type.
 */
case class ParquetType(
    sparkType: DataType,
    descriptor: Option[ColumnDescriptor],
    repetitionLevel: Int,
    definitionLevel: Int,
    required: Boolean,
    path: Seq[String],
    children: Seq[ParquetType]) {

  def isPrimitive: Boolean = descriptor.nonEmpty

  /**
   * Get all the leaves (i.e., primitive columns) of this, in a depth-first order.
   */
  def leaves: Seq[ParquetType] = {
    val buffer = mutable.ArrayBuffer[ParquetType]()
    leaves0(buffer)
    buffer.toSeq
  }

  private def leaves0(buffer: mutable.ArrayBuffer[ParquetType]): Unit = {
    children.foreach(_.leaves0(buffer))
  }
}

object ParquetType {
  def apply(sparkType: DataType, column: PrimitiveColumnIO): ParquetType = {
    this(sparkType, Some(column.getColumnDescriptor), ColumnIOUtil.getRepetitionLevel(column),
      ColumnIOUtil.getDefinitionLevel(column), column.getType.isRepetition(Repetition.REQUIRED),
      ColumnIOUtil.getFieldPath(column), Seq.empty)
  }

  def apply(sparkType: DataType, column: GroupColumnIO, children: Seq[ParquetType]): ParquetType = {
    this(sparkType, None, ColumnIOUtil.getRepetitionLevel(column),
      ColumnIOUtil.getDefinitionLevel(column), column.getType.isRepetition(Repetition.REQUIRED),
      ColumnIOUtil.getFieldPath(column), children)
  }
}