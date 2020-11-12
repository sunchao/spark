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

package org.apache.spark.sql.execution.exchange

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, IcebergBucketTransform, IcebergMonthTransform, IcebergYearTransform, Literal}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.physical.{DataSourcePartitioning, HashPartitioning, PartitioningCollection}
import org.apache.spark.sql.execution.{DummySparkPlan, SortExec}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.test.SharedSparkSession

class EnsureRequirementsSuite extends SharedSparkSession {
  private val exprA = Literal(1)
  private val exprB = Literal(2)
  private val exprC = Literal(3)
  private val exprD = Literal(4)

  test("reorder should handle PartitioningCollection") {
    val plan1 = DummySparkPlan(
      outputPartitioning = PartitioningCollection(Seq(
        HashPartitioning(exprA :: exprB :: Nil, 5),
        HashPartitioning(exprA :: Nil, 5))))
    val plan2 = DummySparkPlan()

    // Test PartitioningCollection on the left side of join.
    val smjExec1 = SortMergeJoinExec(
      exprB :: exprA :: Nil, exprA :: exprB :: Nil, Inner, None, plan1, plan2)
    EnsureRequirements.apply(smjExec1) match {
      case SortMergeJoinExec(leftKeys, rightKeys, _, _,
        SortExec(_, _, DummySparkPlan(_, _, _: PartitioningCollection, _, _), _),
        SortExec(_, _, ShuffleExchangeExec(_: HashPartitioning, _, _), _), _) =>
        assert(leftKeys === Seq(exprA, exprB))
        assert(rightKeys === Seq(exprB, exprA))
      case other => fail(other.toString)
    }

    // Test PartitioningCollection on the right side of join.
    val smjExec2 = SortMergeJoinExec(
      exprA :: exprB :: Nil, exprB :: exprA :: Nil, Inner, None, plan2, plan1)
    EnsureRequirements.apply(smjExec2) match {
      case SortMergeJoinExec(leftKeys, rightKeys, _, _,
        SortExec(_, _, ShuffleExchangeExec(_: HashPartitioning, _, _), _),
        SortExec(_, _, DummySparkPlan(_, _, _: PartitioningCollection, _, _), _), _) =>
        assert(leftKeys === Seq(exprB, exprA))
        assert(rightKeys === Seq(exprA, exprB))
      case other => fail(other.toString)
    }

    // Both sides are PartitioningCollection, but left side cannot be reordered to match
    // and it should fall back to the right side.
    val smjExec3 = SortMergeJoinExec(
      exprA :: exprC :: Nil, exprB :: exprA :: Nil, Inner, None, plan1, plan1)
    EnsureRequirements.apply(smjExec3) match {
      case SortMergeJoinExec(leftKeys, rightKeys, _, _,
        SortExec(_, _, ShuffleExchangeExec(_: HashPartitioning, _, _), _),
        SortExec(_, _, DummySparkPlan(_, _, _: PartitioningCollection, _, _), _), _) =>
        assert(leftKeys === Seq(exprC, exprA))
        assert(rightKeys === Seq(exprA, exprB))
      case other => fail(other.toString)
    }
  }

  test("reorder should fallback to the other side partitioning") {
    val plan1 = DummySparkPlan(
      outputPartitioning = HashPartitioning(exprA :: exprB :: exprC :: Nil, 5))
    val plan2 = DummySparkPlan(
      outputPartitioning = HashPartitioning(exprB :: exprC :: Nil, 5))

    // Test fallback to the right side, which has HashPartitioning.
    val smjExec1 = SortMergeJoinExec(
      exprA :: exprB :: Nil, exprC :: exprB :: Nil, Inner, None, plan1, plan2)
    EnsureRequirements.apply(smjExec1) match {
      case SortMergeJoinExec(leftKeys, rightKeys, _, _,
        SortExec(_, _, ShuffleExchangeExec(_: HashPartitioning, _, _), _),
        SortExec(_, _, DummySparkPlan(_, _, _: HashPartitioning, _, _), _), _) =>
        assert(leftKeys === Seq(exprB, exprA))
        assert(rightKeys === Seq(exprB, exprC))
      case other => fail(other.toString)
    }

    // Test fallback to the right side, which has PartitioningCollection.
    val plan3 = DummySparkPlan(
      outputPartitioning = PartitioningCollection(Seq(HashPartitioning(exprB :: exprC :: Nil, 5))))
    val smjExec2 = SortMergeJoinExec(
      exprA :: exprB :: Nil, exprC :: exprB :: Nil, Inner, None, plan1, plan3)
    EnsureRequirements.apply(smjExec2) match {
      case SortMergeJoinExec(leftKeys, rightKeys, _, _,
        SortExec(_, _, ShuffleExchangeExec(_: HashPartitioning, _, _), _),
        SortExec(_, _, DummySparkPlan(_, _, _: PartitioningCollection, _, _), _), _) =>
        assert(leftKeys === Seq(exprB, exprA))
        assert(rightKeys === Seq(exprB, exprC))
      case other => fail(other.toString)
    }

    // The right side has HashPartitioning, so it is matched first, but no reordering match is
    // found, and it should fall back to the left side, which has a PartitioningCollection.
    val smjExec3 = SortMergeJoinExec(
      exprC :: exprB :: Nil, exprA :: exprB :: Nil, Inner, None, plan3, plan1)
    EnsureRequirements.apply(smjExec3) match {
      case SortMergeJoinExec(leftKeys, rightKeys, _, _,
        SortExec(_, _, DummySparkPlan(_, _, _: PartitioningCollection, _, _), _),
        SortExec(_, _, ShuffleExchangeExec(_: HashPartitioning, _, _), _), _) =>
        assert(leftKeys === Seq(exprB, exprC))
        assert(rightKeys === Seq(exprB, exprA))
      case other => fail(other.toString)
    }
  }

  test("reordering should handle DataSourcePartitioning") {
    // partitioning on the left
    val plan1 = DummySparkPlan(
      outputPartitioning = dsp(Seq(
        IcebergYearTransform(exprA), IcebergBucketTransform(42, exprB),
        IcebergMonthTransform(exprC), exprD))
    )
    val plan2 = DummySparkPlan(
      outputPartitioning = dsp(Seq(
        IcebergYearTransform(exprB), IcebergBucketTransform(42, exprA),
        IcebergMonthTransform(exprD), exprC))
    )
    val smjExec = SortMergeJoinExec(
      exprB :: exprD :: exprC :: exprA :: Nil, exprA :: exprC :: exprD :: exprB :: Nil,
      Inner, None, plan1, plan2
    )
    EnsureRequirements.apply(smjExec) match {
      case SortMergeJoinExec(leftKeys, rightKeys, _, _,
        SortExec(_, _, DummySparkPlan(_, _, _: DataSourcePartitioning, _, _), _),
        SortExec(_, _, DummySparkPlan(_, _, _: DataSourcePartitioning, _, _), _), _) =>
        assert(leftKeys === Seq(exprA, exprB, exprC, exprD))
        assert(rightKeys === Seq(exprB, exprA, exprD, exprC))
      case other => fail(other.toString)
    }

    // partitioning on the right
    val plan3 = DummySparkPlan(
      outputPartitioning = dsp(Seq(
        IcebergBucketTransform(42, exprD), IcebergMonthTransform(exprA),
        IcebergYearTransform(exprC)))
    )
    val smjExec2 = SortMergeJoinExec(
      exprB :: exprD :: exprC :: Nil, exprA :: exprC :: exprD :: Nil,
      Inner, None, plan1, plan3
    )
    EnsureRequirements.apply(smjExec2) match {
      case SortMergeJoinExec(leftKeys, rightKeys, _, _,
        SortExec(_, _, ShuffleExchangeExec(_: HashPartitioning, _, _), _),
        SortExec(_, _, ShuffleExchangeExec(_: HashPartitioning, _, _), _), _) =>
        assert(leftKeys === Seq(exprC, exprB, exprD))
        assert(rightKeys === Seq(exprD, exprA, exprC))
      case other => fail(other.toString)
    }

    // TODO: test transforms with multiple arguments
  }

  test("no shuffle when DataSourcePartitioning is specified") {
    var plan1 = DummySparkPlan(outputPartitioning = dsp(Seq(exprA, exprB)))
    var plan2 = DummySparkPlan(outputPartitioning = dsp(Seq(exprA, exprB)))
    var smjExec = SortMergeJoinExec(
      exprA :: exprB :: Nil, exprA :: exprB :: Nil, Inner, None, plan1, plan2
    )
    EnsureRequirements.apply(smjExec) match {
      case SortMergeJoinExec(_, _, _, _,
         SortExec(_, _, leftChild, _), SortExec(_, _, rightChild, _), _) =>
        assert(leftChild.isInstanceOf[DummySparkPlan])
        assert(leftChild.outputPartitioning.isInstanceOf[DataSourcePartitioning])
        assert(rightChild.isInstanceOf[DummySparkPlan])
        assert(rightChild.outputPartitioning.isInstanceOf[DataSourcePartitioning])
    }

    // more complex cases
    plan1 = DummySparkPlan(outputPartitioning = dsp(
      Seq(IcebergBucketTransform(42, exprA), IcebergMonthTransform(exprB),
        IcebergYearTransform(exprC))))
    plan2 = DummySparkPlan(outputPartitioning = dsp(
      Seq(IcebergBucketTransform(42, exprA), IcebergMonthTransform(exprB),
        IcebergYearTransform(exprC))
    ))
    smjExec = SortMergeJoinExec(
      exprA :: exprB :: exprC :: Nil, exprA :: exprB :: exprC :: Nil,
      Inner, None, plan1, plan2)
    EnsureRequirements.apply(smjExec) match {
      case SortMergeJoinExec(_, _, _, _,
      SortExec(_, _, leftChild, _), SortExec(_, _, rightChild, _), _) =>
        assert(leftChild.isInstanceOf[DummySparkPlan])
        assert(leftChild.outputPartitioning.isInstanceOf[DataSourcePartitioning])
        assert(rightChild.isInstanceOf[DummySparkPlan])
        assert(rightChild.outputPartitioning.isInstanceOf[DataSourcePartitioning])
    }

    // 3-way join
    val plan3 = DummySparkPlan(outputPartitioning = dsp(
      Seq(IcebergBucketTransform(42, exprA), IcebergMonthTransform(exprB),
        IcebergYearTransform(exprC))
    ))
    val smjExec2 = SortMergeJoinExec(
      exprA :: exprB :: exprC :: Nil, exprA :: exprB :: exprC :: Nil,
      Inner, None, smjExec, plan3)
    EnsureRequirements.apply(smjExec2) match {
      case SortMergeJoinExec(_, _, _, _,
      leftChild: SortMergeJoinExec, SortExec(_, _, rightChild, _), _) =>
        assert(leftChild.outputPartitioning.isInstanceOf[PartitioningCollection])
        assert(rightChild.isInstanceOf[DummySparkPlan])
        assert(rightChild.outputPartitioning.isInstanceOf[DataSourcePartitioning])
    }
  }

  test("insert shuffle when DataSourcePartitioning can't be satisfied") {
    val plan1 = DummySparkPlan(outputPartitioning = dsp(Seq(exprA, exprB)))
    val plan2 = DummySparkPlan(outputPartitioning = dsp(Seq(exprA, exprB)))
    var smjExec = SortMergeJoinExec(
      exprA :: exprB :: Nil, exprB :: exprA :: Nil, Inner, None, plan1, plan2
    )
    EnsureRequirements.apply(smjExec) match {
      case SortMergeJoinExec(_, _, _, _,
      SortExec(_, _, leftChild, _), SortExec(_, _, rightChild, _), _) =>
        assert(leftChild.isInstanceOf[ShuffleExchangeExec])
        assert(leftChild.outputPartitioning.isInstanceOf[HashPartitioning])
        assert(rightChild.isInstanceOf[ShuffleExchangeExec])
        assert(rightChild.outputPartitioning.isInstanceOf[HashPartitioning])
    }

    smjExec = SortMergeJoinExec(
      exprA :: exprB :: Nil, exprA :: exprB :: Nil, Inner, None, plan1, plan2
    )
    val plan3 = DummySparkPlan(outputPartitioning = dsp(Seq(exprA, exprB)))
    val smjExec2 = SortMergeJoinExec(
      exprA :: exprB :: Nil, exprB :: exprA :: Nil, Inner, None, smjExec, plan3
    )
    EnsureRequirements.apply(smjExec2) match {
      case SortMergeJoinExec(_, _, _, _,
        SortExec(_, _, left: ShuffleExchangeExec, _),
        SortExec(_, _, right: ShuffleExchangeExec, _), _) =>
        assert(left.outputPartitioning.isInstanceOf[HashPartitioning])
        val leftShuffles = left.child.collect { case s: ShuffleExchangeExec => s }
        assert(leftShuffles.isEmpty)
        assert(right.outputPartitioning.isInstanceOf[HashPartitioning])
    }
  }

  private def dsp(
      clustering: Seq[Expression],
      partitionValues: Seq[InternalRow] = Seq.empty): DataSourcePartitioning = {
    DataSourcePartitioning(clustering, partitionValues)
  }
}
