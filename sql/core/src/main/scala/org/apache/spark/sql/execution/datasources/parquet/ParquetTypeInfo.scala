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
import org.apache.parquet.io.{ColumnIOUtil, GroupColumnIO, PrimitiveColumnIO}
import org.apache.parquet.schema.Type.Repetition

import org.apache.spark.sql.types.DataType

trait ParquetReadInfo {
  def sparkType: DataType
  def repetitionLevel: Int
  def definitionLevel: Int
  def required: Boolean

  def withNewType(ty: DataType): ParquetReadInfo = this match {
    case ParquetGroupReadInfo(_, repetitionLevel, definitionLevel, required, children) =>
      ParquetGroupReadInfo(ty, repetitionLevel, definitionLevel, required, children)
    case ParquetPrimitiveReadInfo(_, desc, repetitionLevel, definitionLevel, required) =>
      ParquetPrimitiveReadInfo(ty, desc, repetitionLevel, definitionLevel, required)
  }

  def isPrimitive: Boolean = this match {
    case _: ParquetPrimitiveReadInfo => true
    case _ => false
  }

  /**
   * Get all the leaves (i.e., primitive columns) of this, in a depth-first order.
   * @return
   */
  def leaves: Seq[ParquetPrimitiveReadInfo] = {
    val buffer = mutable.ArrayBuffer[ParquetPrimitiveReadInfo]()
    leaves0(buffer)
    buffer.toSeq
  }

  private def leaves0(buffer: mutable.ArrayBuffer[ParquetPrimitiveReadInfo]): Unit = this match {
    case info: ParquetPrimitiveReadInfo =>
      buffer.append(info)
    case info: ParquetGroupReadInfo =>
      info.children.foreach(_.leaves0(buffer))
  }
}

case class ParquetPrimitiveReadInfo(
    sparkType: DataType,
    descriptor: ColumnDescriptor,
    repetitionLevel: Int,
    definitionLevel: Int,
    required: Boolean)
  extends ParquetReadInfo

object ParquetPrimitiveReadInfo {
  def apply(sparkType: DataType, column: PrimitiveColumnIO): ParquetPrimitiveReadInfo = {
    this(sparkType, column.getColumnDescriptor, ColumnIOUtil.getRepetitionLevel(column),
      ColumnIOUtil.getDefinitionLevel(column), !column.getType.isRepetition(Repetition.OPTIONAL))
  }
}

case class ParquetGroupReadInfo(
    sparkType: DataType,
    repetitionLevel: Int,
    definitionLevel: Int,
    required: Boolean,
    children: Seq[ParquetReadInfo])
  extends ParquetReadInfo

object ParquetGroupReadInfo {
  def apply(
      sparkType: DataType,
      column: GroupColumnIO,
      children: Seq[ParquetReadInfo]): ParquetGroupReadInfo = {
    this(sparkType, ColumnIOUtil.getRepetitionLevel(column),
      ColumnIOUtil.getDefinitionLevel(column), !column.getType.isRepetition(Repetition.OPTIONAL),
      children)
  }
}

