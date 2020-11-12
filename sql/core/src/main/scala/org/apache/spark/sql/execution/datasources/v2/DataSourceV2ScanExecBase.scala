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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{RowOrdering, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, DataSourcePartitioning, Distribution, SinglePartition}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.connector.read.{HasPartitionKey, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.{ExplainUtils, LeafExecNode}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.connector.SupportsMetadata
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

trait DataSourceV2ScanExecBase extends LeafExecNode {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  def scan: Scan

  def readerFactory: PartitionReaderFactory

  /** Optional distribution guarantee provided by the V2 data source */
  def distribution: Option[Distribution]

  /** Optional (empty means no ordering) guarantee provided by the V2 data source */
  def ordering: Seq[SortOrder]

  override def simpleString(maxFields: Int): String = {
    val result =
      s"$nodeName${truncatedString(output, "[", ", ", "]", maxFields)} ${scan.description()}"
    redact(result)
  }

  protected def inputPartitions(): Seq[InputPartition]

  /**
   * Shorthand for calling redact() without specifying redacting rules
   */
  protected def redact(text: String): String = {
    Utils.redact(sqlContext.sessionState.conf.stringRedactionPattern, text)
  }

  override def verboseStringWithOperatorId(): String = {
    val metaDataStr = scan match {
      case s: SupportsMetadata =>
        s.getMetaData().toSeq.sorted.flatMap {
          case (_, value) if value.isEmpty || value.equals("[]") => None
          case (key, value) => Some(s"$key: ${redact(value)}")
          case _ => None
        }
      case _ =>
        Seq(scan.description())
    }
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", output)}
       |${metaDataStr.mkString("\n")}
       |""".stripMargin
  }

  override def outputPartitioning: physical.Partitioning = {
    if (partitions.length == 1) {
      SinglePartition
    } else if (partitionValues.isDefined) {
      val clustering = distribution.get.asInstanceOf[ClusteredDistribution].clustering
      DataSourcePartitioning(clustering, partitionValues.get.map(_._2))
    } else {
      super.outputPartitioning
    }
  }

  override def outputOrdering: Seq[SortOrder] = ordering

  @transient lazy val partitions: Seq[InputPartition] = {
    if (conf.bucketingEnabled && partitionValues.isDefined) {
      partitionValues.get.map(_._1)
    } else {
      inputPartitions()
    }
  }

  /**
   * Partition values for all the input partitions.
   *
   * NOTE: this is defined iff:
   *   1. all input partitions implement [[HasPartitionKey]]
   *   2. each input partition has a unique partition value.
   *   3. `distribution` is a [[ClusteredDistribution]]
   *
   * Otherwise, the value is None.
   *
   * A non-empty result means each partition is clustered on a single key and therefore eligible
   * for further optimizations to eliminate shuffling in some operations such as join and aggregate.
   *
   * This should only be called if `conf.bucketingEnabled` is on. It could potentially be
   * expensive since it needs to sort all the input partitions according to their keys.
   *
   * TODO: data sources should be able to plan multiple partitions with the same key and Spark
   *   should choose to combine them whenever necessary
   */
  @transient lazy val partitionValues: Option[Seq[(InputPartition, InternalRow)]] = {
    val result = new ArrayBuffer[(InputPartition, InternalRow)]()
    val seen = mutable.HashSet[InternalRow]()
    val partitions = inputPartitions()
    for (p <- partitions) {
      p match {
        case hp: HasPartitionKey if !seen.contains(hp.partitionKey()) =>
          result += ((p, hp.partitionKey))
          seen += hp.partitionKey
        case _ =>
      }
    }

    if (result.nonEmpty && result.length == partitions.length &&
      distribution.isDefined && distribution.get.isInstanceOf[ClusteredDistribution]) {
      // also sort the input partitions according to their partition key order. This ensures
      // a canonical order from both sides of a bucketed join, for example.
      val clustering = distribution.get.asInstanceOf[ClusteredDistribution].clustering
      val keyOrdering: Ordering[(InputPartition, InternalRow)] =
        RowOrdering.createNaturalAscendingOrdering(clustering.map(_.dataType)).on(_._2)
      Some(result.sorted(keyOrdering))
    } else {
      None
    }
  }

  override def supportsColumnar: Boolean = {
    require(partitions.forall(readerFactory.supportColumnarReads) ||
      !partitions.exists(readerFactory.supportColumnarReads),
      "Cannot mix row-based and columnar input partitions.")

    partitions.exists(readerFactory.supportColumnarReads)
  }

  def inputRDD: RDD[InternalRow]

  def inputRDDs(): Seq[RDD[InternalRow]] = Seq(inputRDD)

  override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    inputRDD.map { r =>
      numOutputRows += 1
      r
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    inputRDD.asInstanceOf[RDD[ColumnarBatch]].map { b =>
      numOutputRows += b.numRows()
      b
    }
  }
}
