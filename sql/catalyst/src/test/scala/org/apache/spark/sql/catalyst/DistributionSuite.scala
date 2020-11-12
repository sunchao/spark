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

package org.apache.spark.sql.catalyst

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._

class DistributionSuite extends SparkFunSuite {

  protected def checkSatisfied(
      inputPartitioning: Partitioning,
      requiredDistribution: Distribution,
      satisfied: Boolean): Unit = {
    if (inputPartitioning.satisfies(requiredDistribution) != satisfied) {
      fail(
        s"""
        |== Input Partitioning ==
        |$inputPartitioning
        |== Required Distribution ==
        |$requiredDistribution
        |== Does input partitioning satisfy required distribution? ==
        |Expected $satisfied got ${inputPartitioning.satisfies(requiredDistribution)}
        """.stripMargin)
    }
  }

  protected def checkCompatible(
      left: Partitioning,
      right: Partitioning,
      compatible: Boolean): Unit = {
    if (left.isCompatibleWith(right) != compatible) {
      fail(
        s"""
           |== Left Partitioning ==
           |$left
           |== Right Partitioning ==
           |$right
           |== Is left partitioning compatible with right partitioning? ==
           |Expected $compatible but got ${left.isCompatibleWith(right)}
           |""".stripMargin)
    }
  }

  test("UnspecifiedDistribution and AllTuples") {
    // except `BroadcastPartitioning`, all other partitioning can satisfy UnspecifiedDistribution
    checkSatisfied(
      UnknownPartitioning(-1),
      UnspecifiedDistribution,
      true)

    checkSatisfied(
      RoundRobinPartitioning(10),
      UnspecifiedDistribution,
      true)

    checkSatisfied(
      SinglePartition,
      UnspecifiedDistribution,
      true)

    checkSatisfied(
      HashPartitioning(Seq($"a"), 10),
      UnspecifiedDistribution,
      true)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc), 10),
      UnspecifiedDistribution,
      true)

    checkSatisfied(
      BroadcastPartitioning(IdentityBroadcastMode),
      UnspecifiedDistribution,
      false)

    // except `BroadcastPartitioning`, all other partitioning can satisfy AllTuples if they have
    // only one partition.
    checkSatisfied(
      UnknownPartitioning(1),
      AllTuples,
      true)

    checkSatisfied(
      UnknownPartitioning(10),
      AllTuples,
      false)

    checkSatisfied(
      RoundRobinPartitioning(1),
      AllTuples,
      true)

    checkSatisfied(
      RoundRobinPartitioning(10),
      AllTuples,
      false)

    checkSatisfied(
      SinglePartition,
      AllTuples,
      true)

    checkSatisfied(
      HashPartitioning(Seq($"a"), 1),
      AllTuples,
      true)

    checkSatisfied(
      HashPartitioning(Seq($"a"), 10),
      AllTuples,
      false)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc), 1),
      AllTuples,
      true)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc), 10),
      AllTuples,
      false)

    checkSatisfied(
      BroadcastPartitioning(IdentityBroadcastMode),
      AllTuples,
      false)
  }

  test("SinglePartition is the output partitioning") {
    // SinglePartition can satisfy all the distributions except `BroadcastDistribution`
    checkSatisfied(
      SinglePartition,
      ClusteredDistribution(Seq($"a", $"b", $"c")),
      true)

    checkSatisfied(
      SinglePartition,
      HashClusteredDistribution(Seq($"a", $"b", $"c")),
      true)

    checkSatisfied(
      SinglePartition,
      OrderedDistribution(Seq($"a".asc, $"b".asc, $"c".asc)),
      true)

    checkSatisfied(
      SinglePartition,
      BroadcastDistribution(IdentityBroadcastMode),
      false)
  }

  test("HashPartitioning is the output partitioning") {
    // HashPartitioning can satisfy ClusteredDistribution iff its hash expressions are a subset of
    // the required clustering expressions.
    checkSatisfied(
      HashPartitioning(Seq($"a", $"b", $"c"), 10),
      ClusteredDistribution(Seq($"a", $"b", $"c")),
      true)

    checkSatisfied(
      HashPartitioning(Seq($"b", $"c"), 10),
      ClusteredDistribution(Seq($"a", $"b", $"c")),
      true)

    checkSatisfied(
      HashPartitioning(Seq($"a", $"b", $"c"), 10),
      ClusteredDistribution(Seq($"b", $"c")),
      false)

    checkSatisfied(
      HashPartitioning(Seq($"a", $"b", $"c"), 10),
      ClusteredDistribution(Seq($"d", $"e")),
      false)

    // HashPartitioning can satisfy HashClusteredDistribution iff its hash expressions are exactly
    // same with the required hash clustering expressions.
    checkSatisfied(
      HashPartitioning(Seq($"a", $"b", $"c"), 10),
      HashClusteredDistribution(Seq($"a", $"b", $"c")),
      true)

    checkSatisfied(
      HashPartitioning(Seq($"c", $"b", $"a"), 10),
      HashClusteredDistribution(Seq($"a", $"b", $"c")),
      false)

    checkSatisfied(
      HashPartitioning(Seq($"a", $"b"), 10),
      HashClusteredDistribution(Seq($"a", $"b", $"c")),
      false)

    // HashPartitioning cannot satisfy OrderedDistribution
    checkSatisfied(
      HashPartitioning(Seq($"a", $"b", $"c"), 10),
      OrderedDistribution(Seq($"a".asc, $"b".asc, $"c".asc)),
      false)

    checkSatisfied(
      HashPartitioning(Seq($"a", $"b", $"c"), 1),
      OrderedDistribution(Seq($"a".asc, $"b".asc, $"c".asc)),
      false) // TODO: this can be relaxed.

    checkSatisfied(
      HashPartitioning(Seq($"b", $"c"), 10),
      OrderedDistribution(Seq($"a".asc, $"b".asc, $"c".asc)),
      false)
  }

  test("RangePartitioning is the output partitioning") {
    // RangePartitioning can satisfy OrderedDistribution iff its ordering is a prefix
    // of the required ordering, or the required ordering is a prefix of its ordering.
    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      OrderedDistribution(Seq($"a".asc, $"b".asc, $"c".asc)),
      true)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      OrderedDistribution(Seq($"a".asc, $"b".asc)),
      true)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      OrderedDistribution(Seq($"a".asc, $"b".asc, $"c".asc, $"d".desc)),
      true)

    // TODO: We can have an optimization to first sort the dataset
    // by a.asc and then sort b, and c in a partition. This optimization
    // should tradeoff the benefit of a less number of Exchange operators
    // and the parallelism.
    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      OrderedDistribution(Seq($"a".asc, $"b".desc, $"c".asc)),
      false)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      OrderedDistribution(Seq($"b".asc, $"a".asc)),
      false)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      OrderedDistribution(Seq($"a".asc, $"b".asc, $"d".desc)),
      false)

    // RangePartitioning can satisfy ClusteredDistribution iff its ordering expressions are a subset
    // of the required clustering expressions.
    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      ClusteredDistribution(Seq($"a", $"b", $"c")),
      true)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      ClusteredDistribution(Seq($"c", $"b", $"a")),
      true)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      ClusteredDistribution(Seq($"b", $"c", $"a", $"d")),
      true)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      ClusteredDistribution(Seq($"a", $"b")),
      false)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      ClusteredDistribution(Seq($"c", $"d")),
      false)

    // RangePartitioning cannot satisfy HashClusteredDistribution
    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      HashClusteredDistribution(Seq($"a", $"b", $"c")),
      false)
  }

  test("Partitioning.numPartitions must match Distribution.requiredNumPartitions to satisfy it") {
    checkSatisfied(
      SinglePartition,
      ClusteredDistribution(Seq($"a", $"b", $"c"), Some(10)),
      false)

    checkSatisfied(
      SinglePartition,
      HashClusteredDistribution(Seq($"a", $"b", $"c"), Some(10)),
      false)

    checkSatisfied(
      HashPartitioning(Seq($"a", $"b", $"c"), 10),
      ClusteredDistribution(Seq($"a", $"b", $"c"), Some(5)),
      false)

    checkSatisfied(
      HashPartitioning(Seq($"a", $"b", $"c"), 10),
      HashClusteredDistribution(Seq($"a", $"b", $"c"), Some(5)),
      false)

    checkSatisfied(
      RangePartitioning(Seq($"a".asc, $"b".asc, $"c".asc), 10),
      ClusteredDistribution(Seq($"a", $"b", $"c"), Some(5)),
      false)
  }

  test("DataSourcePartitioning satisfies and compatibility") {
    checkSatisfied(
      DataSourcePartitioning(Seq($"a", $"b", $"c"),
        Seq(InternalRow(0, 0, 0), InternalRow(1, 1, 1))),
      HashClusteredDistribution(Seq($"a", $"b", $"c"), None),
      satisfied = true
    )

    // num partitions is not checked - for now
    checkSatisfied(
      DataSourcePartitioning(Seq($"a", $"b", $"c"),
        Seq(InternalRow(0, 0, 0), InternalRow(1, 1, 1))),
      HashClusteredDistribution(Seq($"a", $"b", $"c"), None),
      satisfied = true
    )

    // should also handle `ClusteredDistribution`
    checkSatisfied(
      DataSourcePartitioning(Seq($"a", $"b", $"c"),
        Seq(InternalRow(0, 0, 0), InternalRow(1, 1, 1))),
      ClusteredDistribution(Seq($"a", $"b", $"c"), None),
      satisfied = true
    )

    // requires exact match between partitioning and distribution
    checkSatisfied(
      DataSourcePartitioning(Seq($"a", $"b"),
        Seq(InternalRow(0, 0, 0), InternalRow(1, 1, 1))),
      HashClusteredDistribution(Seq($"a", $"b", $"c"), None),
      satisfied = false
    )

    // should satisify `UnspecifiedDistribution`
    checkSatisfied(
      DataSourcePartitioning(Seq($"a", $"b", $"c"),
        Seq(InternalRow(0, 0, 0), InternalRow(1, 1, 1))),
      UnspecifiedDistribution,
      satisfied = true
    )

    // unsupported distribution types

    checkSatisfied(
      DataSourcePartitioning(Seq($"a"), Seq.empty),
      BroadcastDistribution(IdentityBroadcastMode),
      satisfied = false
    )

    checkSatisfied(
      DataSourcePartitioning(Seq($"a"), Seq.empty),
      OrderedDistribution(Seq($"a".asc)),
      satisfied = false
    )
  }

  test("isCompatibleWith() for DataSourcePartitioning") {
    checkCompatible(
      DataSourcePartitioning(Seq($"a", $"b", $"c"), Seq.empty),
      DataSourcePartitioning(Seq($"a", $"b", $"c"), Seq.empty),
      compatible = true
    )

    checkCompatible(
      DataSourcePartitioning(Seq(IcebergBucketTransform(42, $"a"), $"c"), Seq.empty),
      DataSourcePartitioning(Seq(IcebergBucketTransform(42, $"d"), $"f"), Seq.empty),
      compatible = true
    )

    checkCompatible(
      DataSourcePartitioning(Seq(IcebergBucketTransform(42, $"a"), IcebergYearTransform($"c")),
        Seq(InternalRow(0, 0, 0), InternalRow(1, 1, 1))),
      DataSourcePartitioning(Seq(IcebergBucketTransform(42, $"d"), IcebergYearTransform($"f")),
        Seq(InternalRow(0, 0, 0), InternalRow(1, 1, 1))),
      compatible = true
    )

    // negative cases

    checkCompatible(
      DataSourcePartitioning(Seq($"a", $"b", $"c"), Seq.empty),
      DataSourcePartitioning(Seq($"a", $"b"), Seq.empty),
      compatible = false
    )

    checkCompatible(
      DataSourcePartitioning(Seq($"a", $"b", $"c"), Seq(InternalRow(0, 0, 0))),
      DataSourcePartitioning(Seq($"a", $"b", $"c"),
        Seq(InternalRow(0, 0, 0), InternalRow(1, 1, 1))),
      compatible = false
    )

    checkCompatible(
      DataSourcePartitioning(Seq(IcebergBucketTransform(42, $"a"),
        IcebergYearTransform($"c")), Seq.empty),
      DataSourcePartitioning(Seq(IcebergBucketTransform(42, $"a"),
        IcebergMonthTransform($"c")), Seq.empty),
      compatible = false
    )

    checkCompatible(
      DataSourcePartitioning(Seq(IcebergBucketTransform(42, $"a"), $"c"),
        Seq(InternalRow(0, 0, 0), InternalRow(1, 1, 1))),
      DataSourcePartitioning(Seq(IcebergBucketTransform(42, $"a"), $"c"),
        Seq(InternalRow(0, 0, 0), InternalRow(2, 2, 2))),
      compatible = false
    )
  }

  test("isCompatibleWith() for PartitioningCollection") {
    checkCompatible(
      DataSourcePartitioning(Seq($"a"), Seq.empty),
      PartitioningCollection(Seq(DataSourcePartitioning(Seq($"a"), Seq.empty))),
      compatible = true
    )

    checkCompatible(
      DataSourcePartitioning(Seq($"a"), Seq.empty),
      PartitioningCollection(Seq(
        DataSourcePartitioning(Seq($"a"), Seq.empty),
        DataSourcePartitioning(Seq(IcebergBucketTransform(32, $"b")), Seq.empty))),
      compatible = true
    )

    checkCompatible(
      DataSourcePartitioning(Seq($"a"), Seq.empty),
      PartitioningCollection(Seq(
        DataSourcePartitioning(Seq(IcebergBucketTransform(32, $"b")), Seq.empty),
        DataSourcePartitioning(Seq($"a"), Seq.empty))),
      compatible = true
    )

    checkCompatible(
      DataSourcePartitioning(Seq(IcebergYearTransform($"a"), IcebergBucketTransform(32, $"b")),
        Seq(InternalRow("2020-01-01", 1), InternalRow("1900-01-01", 2))),
      PartitioningCollection(Seq(
        HashPartitioning(Seq($"a"), 2),
        DataSourcePartitioning(Seq(IcebergYearTransform($"a"), IcebergBucketTransform(32, $"b")),
          Seq(InternalRow("2020-01-01", 1), InternalRow("1900-01-01", 2))))),
      compatible = true
    )

    checkCompatible(
      PartitioningCollection(Seq(DataSourcePartitioning(Seq($"a"), Seq.empty))),
      DataSourcePartitioning(Seq($"a"), Seq.empty),
      compatible = true
    )

    checkCompatible(
      PartitioningCollection(Seq(
        DataSourcePartitioning(Seq($"a"), Seq.empty),
        DataSourcePartitioning(Seq(IcebergBucketTransform(32, $"b")), Seq.empty))),
      DataSourcePartitioning(Seq($"a"), Seq.empty),
      compatible = true
    )

    checkCompatible(
      PartitioningCollection(Seq(
        DataSourcePartitioning(Seq(IcebergBucketTransform(32, $"b")), Seq.empty),
        DataSourcePartitioning(Seq($"a"), Seq.empty))),
      DataSourcePartitioning(Seq($"a"), Seq.empty),
      compatible = true
    )

    checkCompatible(
      PartitioningCollection(Seq(
        HashPartitioning(Seq($"a"), 2),
        DataSourcePartitioning(Seq(IcebergYearTransform($"a"), IcebergBucketTransform(32, $"b")),
          Seq(InternalRow("2020-01-01", 1), InternalRow("1900-01-01", 2))))),
      DataSourcePartitioning(Seq(IcebergYearTransform($"a"), IcebergBucketTransform(32, $"b")),
        Seq(InternalRow("2020-01-01", 1), InternalRow("1900-01-01", 2))),
      compatible = true
    )

    checkCompatible(
      DataSourcePartitioning(Seq(IcebergYearTransform($"a")), Seq.empty),
      PartitioningCollection(Seq(
        DataSourcePartitioning(Seq(IcebergHourTransform($"a")), Seq.empty),
        DataSourcePartitioning(Seq(IcebergDayTransform($"a")), Seq.empty))),
      compatible = false
    )

    checkCompatible(
      PartitioningCollection(Seq(
        DataSourcePartitioning(Seq(IcebergHourTransform($"a")), Seq.empty),
        DataSourcePartitioning(Seq(IcebergDayTransform($"a")), Seq.empty))),
      DataSourcePartitioning(Seq(IcebergYearTransform($"a")), Seq.empty),
      compatible = false
    )

    checkCompatible(
      PartitioningCollection(Seq(
        DataSourcePartitioning(Seq(IcebergHourTransform($"a")),
          Seq(InternalRow("2020-01-01", 1), InternalRow("1900-01-01", 2))),
        HashPartitioning(Seq(IcebergYearTransform($"a")), 2))),
      DataSourcePartitioning(Seq(IcebergYearTransform($"a")),
        Seq(InternalRow("2020-01-01", 1), InternalRow("1900-01-01", 2))),
      compatible = false
    )

    checkCompatible(
      DataSourcePartitioning(Seq(IcebergYearTransform($"a")),
        Seq(InternalRow("2020-01-01", 1), InternalRow("1900-01-01", 2))),
      PartitioningCollection(Seq(
        DataSourcePartitioning(Seq(IcebergHourTransform($"a")),
          Seq(InternalRow("2020-01-01", 1), InternalRow("1900-01-01", 2))),
        HashPartitioning(Seq(IcebergYearTransform($"a")), 2))),
      compatible = false
    )
  }

  test("isCompatibleWith() should always succeed for non-data source partitionings") {
    val partitionings: Seq[Partitioning] = Seq(UnknownPartitioning(1),
      BroadcastPartitioning(IdentityBroadcastMode),
      RoundRobinPartitioning(10),
      HashPartitioning(Seq($"a"), 10),
      RangePartitioning(Seq($"a".asc), 10),
      PartitioningCollection(Seq(UnknownPartitioning(1)))
    )

    for (i <- partitionings.indices) {
      for (j <- partitionings.indices) {
        checkCompatible(partitionings(i), partitionings(j), compatible = true)
      }
    }

    // should always return false when comparing with `DataSourcePartitioning`
    partitionings.foreach { p =>
      checkCompatible(p, DataSourcePartitioning(Seq($"a"), Seq.empty),
        compatible = false)
    }
  }
}
