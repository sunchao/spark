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

/**
 * Rich type information for a Parquet type together with its SparkSQL type.
 */
trait ParquetType {
  def sparkType: DataType
  def repetitionLevel: Int
  def definitionLevel: Int
  def required: Boolean
  def path: Seq[String]

  def withNewType(dt: DataType): ParquetType = this match {
    case ParquetComplexType(_, repetitionLevel, definitionLevel, required, path, children) =>
      ParquetComplexType(dt, repetitionLevel, definitionLevel, required, path, children)
    case ParquetPrimitiveType(_, desc, repetitionLevel, definitionLevel, required, path) =>
      ParquetPrimitiveType(dt, desc, repetitionLevel, definitionLevel, required, path)
  }

  def isPrimitive: Boolean = this match {
    case _: ParquetPrimitiveType => true
    case _ => false
  }

  /**
   * Get all the leaves (i.e., primitive columns) of this, in a depth-first order.
   */
  def leaves: Seq[ParquetPrimitiveType] = {
    val buffer = mutable.ArrayBuffer[ParquetPrimitiveType]()
    leaves0(buffer)
    buffer.toSeq
  }

  private def leaves0(buffer: mutable.ArrayBuffer[ParquetPrimitiveType]): Unit = this match {
    case info: ParquetPrimitiveType =>
      buffer.append(info)
    case info: ParquetComplexType =>
      info.children.foreach(_.leaves0(buffer))
  }
}

case class ParquetPrimitiveType(
    sparkType: DataType,
    descriptor: ColumnDescriptor,
    repetitionLevel: Int,
    definitionLevel: Int,
    required: Boolean,
    path: Seq[String])
  extends ParquetType

object ParquetPrimitiveType {
  def apply(sparkType: DataType, column: PrimitiveColumnIO): ParquetPrimitiveType = {
    this(sparkType, column.getColumnDescriptor, ColumnIOUtil.getRepetitionLevel(column),
      ColumnIOUtil.getDefinitionLevel(column), column.getType.isRepetition(Repetition.REQUIRED),
      ColumnIOUtil.getFieldPath(column))
  }
}

/**
 * Represents a Parquet complex type, e.g., list, struct, map.
 */
case class ParquetComplexType(
    sparkType: DataType,
    repetitionLevel: Int,
    definitionLevel: Int,
    required: Boolean,
    path: Seq[String],
    children: Seq[ParquetType])
  extends ParquetType

object ParquetComplexType {
  def apply(
      sparkType: DataType,
      column: GroupColumnIO,
      children: Seq[ParquetType]): ParquetComplexType = {
    this(sparkType, ColumnIOUtil.getRepetitionLevel(column),
      ColumnIOUtil.getDefinitionLevel(column), column.getType.isRepetition(Repetition.REQUIRED),
      ColumnIOUtil.getFieldPath(column), children)
  }
}

