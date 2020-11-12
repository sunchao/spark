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

package org.apache.spark.sql.catalyst.plans.physical

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical
import org.apache.spark.sql.types.{DataType, IntegerType}

/**
 * Specifies how tuples that share common expressions will be distributed when a query is executed
 * in parallel on many machines.
 *
 * Distribution here refers to inter-node partitioning of data. That is, it describes how tuples
 * are partitioned across physical machines in a cluster. Knowing this property allows some
 * operators (e.g., Aggregate) to perform partition local operations instead of global ones.
 */
sealed trait Distribution {
  /**
   * The required number of partitions for this distribution. If it's None, then any number of
   * partitions is allowed for this distribution.
   */
  def requiredNumPartitions: Option[Int]

  /**
   * Creates a default partitioning for this distribution, which can satisfy this distribution while
   * matching the given number of partitions.
   */
  def createPartitioning(numPartitions: Int): Partitioning
}

/**
 * Represents a distribution where no promises are made about co-location of data.
 */
case object UnspecifiedDistribution extends Distribution {
  override def requiredNumPartitions: Option[Int] = None

  override def createPartitioning(numPartitions: Int): Partitioning = {
    throw new IllegalStateException("UnspecifiedDistribution does not have default partitioning.")
  }
}

/**
 * Represents a distribution that only has a single partition and all tuples of the dataset
 * are co-located.
 */
case object AllTuples extends Distribution {
  override def requiredNumPartitions: Option[Int] = Some(1)

  override def createPartitioning(numPartitions: Int): Partitioning = {
    assert(numPartitions == 1, "The default partitioning of AllTuples can only have 1 partition.")
    SinglePartition
  }
}

/**
 * Represents data where tuples that share the same values for the `clustering`
 * [[Expression Expressions]] will be co-located in the same partition.
 */
case class ClusteredDistribution(
    clustering: Seq[Expression],
    requiredNumPartitions: Option[Int] = None) extends Distribution {
  require(
    clustering != Nil,
    "The clustering expressions of a ClusteredDistribution should not be Nil. " +
      "An AllTuples should be used to represent a distribution that only has " +
      "a single partition.")

  override def createPartitioning(numPartitions: Int): Partitioning = {
    assert(requiredNumPartitions.isEmpty || requiredNumPartitions.get == numPartitions,
      s"This ClusteredDistribution requires ${requiredNumPartitions.get} partitions, but " +
        s"the actual number of partitions is $numPartitions.")
    HashPartitioning(clustering, numPartitions)
  }
}

/**
 * Represents data where tuples have been clustered according to the hash of the given
 * `expressions`. The hash function is defined as `HashPartitioning.partitionIdExpression`, so only
 * [[HashPartitioning]] can satisfy this distribution.
 *
 * This is a strictly stronger guarantee than [[ClusteredDistribution]]. Given a tuple and the
 * number of partitions, this distribution strictly requires which partition the tuple should be in.
 */
case class HashClusteredDistribution(
    expressions: Seq[Expression],
    requiredNumPartitions: Option[Int] = None) extends Distribution {
  require(
    expressions != Nil,
    "The expressions for hash of a HashClusteredDistribution should not be Nil. " +
      "An AllTuples should be used to represent a distribution that only has " +
      "a single partition.")

  override def createPartitioning(numPartitions: Int): Partitioning = {
    assert(requiredNumPartitions.isEmpty || requiredNumPartitions.get == numPartitions,
      s"This HashClusteredDistribution requires ${requiredNumPartitions.get} partitions, but " +
        s"the actual number of partitions is $numPartitions.")
    HashPartitioning(expressions, numPartitions)
  }
}

/**
 * Represents data where tuples have been ordered according to the `ordering`
 * [[Expression Expressions]]. Its requirement is defined as the following:
 *   - Given any 2 adjacent partitions, all the rows of the second partition must be larger than or
 *     equal to any row in the first partition, according to the `ordering` expressions.
 *
 * In other words, this distribution requires the rows to be ordered across partitions, but not
 * necessarily within a partition.
 */
case class OrderedDistribution(ordering: Seq[SortOrder]) extends Distribution {
  require(
    ordering != Nil,
    "The ordering expressions of an OrderedDistribution should not be Nil. " +
      "An AllTuples should be used to represent a distribution that only has " +
      "a single partition.")

  override def requiredNumPartitions: Option[Int] = None

  override def createPartitioning(numPartitions: Int): Partitioning = {
    RangePartitioning(ordering, numPartitions)
  }
}

/**
 * Represents data where tuples are broadcasted to every node. It is quite common that the
 * entire set of tuples is transformed into different data structure.
 */
case class BroadcastDistribution(mode: BroadcastMode) extends Distribution {
  override def requiredNumPartitions: Option[Int] = Some(1)

  override def createPartitioning(numPartitions: Int): Partitioning = {
    assert(numPartitions == 1,
      "The default partitioning of BroadcastDistribution can only have 1 partition.")
    BroadcastPartitioning(mode)
  }
}

/**
 * Describes how an operator's output is split across partitions. It has 2 major properties:
 *   1. number of partitions.
 *   2. if it can satisfy a given distribution.
 */
trait Partitioning {
  /** Returns the number of partitions that the data is split across */
  val numPartitions: Int

  /**
   * Returns true iff the guarantees made by this [[Partitioning]] are sufficient
   * to satisfy the partitioning scheme mandated by the `required` [[Distribution]],
   * i.e. the current dataset does not need to be re-partitioned for the `required`
   * Distribution (it is possible that tuples within a partition need to be reorganized).
   *
   * A [[Partitioning]] can never satisfy a [[Distribution]] if its `numPartitions` doesn't match
   * [[Distribution.requiredNumPartitions]].
   */
  final def satisfies(required: Distribution): Boolean = {
    required.requiredNumPartitions.forall(_ == numPartitions) && satisfies0(required)
  }

  /**
   * The actual method that defines whether this [[Partitioning]] can satisfy the given
   * [[Distribution]], after the `numPartitions` check.
   *
   * By default a [[Partitioning]] can satisfy [[UnspecifiedDistribution]], and [[AllTuples]] if
   * the [[Partitioning]] only have one partition. Implementations can also overwrite this method
   * with special logic.
   */
  protected def satisfies0(required: Distribution): Boolean = required match {
    case UnspecifiedDistribution => true
    case AllTuples => numPartitions == 1
    case _ => false
  }

  /**
   * Whether this partitioning is compatible with the other partitioning provided. Note this
   * operation MUST be transitive.
   *
   * Currently this only checked for [[DataSourcePartitioning]] where we ensure both partitionings
   * have the same transforms. For others, this simply returns true for non-data source
   * partitioning and false otherwise.
   */
  def isCompatibleWith(other: Partitioning): Boolean = other match {
    case _: DataSourcePartitioning => false
    case _ => true
  }
}

case class UnknownPartitioning(numPartitions: Int) extends Partitioning

/**
 * Represents a partitioning where rows are distributed evenly across output partitions
 * by starting from a random target partition number and distributing rows in a round-robin
 * fashion. This partitioning is used when implementing the DataFrame.repartition() operator.
 */
case class RoundRobinPartitioning(numPartitions: Int) extends Partitioning

case object SinglePartition extends Partitioning {
  val numPartitions = 1

  override def satisfies0(required: Distribution): Boolean = required match {
    case _: BroadcastDistribution => false
    case _ => true
  }
}

/**
 * Represents a partitioning where rows are split up across partitions based on the hash
 * of `expressions`.  All rows where `expressions` evaluate to the same values are guaranteed to be
 * in the same partition.
 */
case class HashPartitioning(expressions: Seq[Expression], numPartitions: Int)
  extends Expression with Partitioning with Unevaluable {

  override def children: Seq[Expression] = expressions
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def satisfies0(required: Distribution): Boolean = {
    super.satisfies0(required) || {
      required match {
        case h: HashClusteredDistribution =>
          expressions.length == h.expressions.length && expressions.zip(h.expressions).forall {
            case (l, r) => l.semanticEquals(r)
          }
        case ClusteredDistribution(requiredClustering, _) =>
          expressions.forall(x => requiredClustering.exists(_.semanticEquals(x)))
        case _ => false
      }
    }
  }

  /**
   * Returns an expression that will produce a valid partition ID(i.e. non-negative and is less
   * than numPartitions) based on hashing expressions.
   */
  def partitionIdExpression: Expression = Pmod(new Murmur3Hash(expressions), Literal(numPartitions))
}

/**
 * Represents a partitioning where rows are split across partitions based on some total ordering of
 * the expressions specified in `ordering`.  When data is partitioned in this manner, it guarantees:
 * Given any 2 adjacent partitions, all the rows of the second partition must be larger than any row
 * in the first partition, according to the `ordering` expressions.
 *
 * This is a strictly stronger guarantee than what `OrderedDistribution(ordering)` requires, as
 * there is no overlap between partitions.
 *
 * This class extends expression primarily so that transformations over expression will descend
 * into its child.
 */
case class RangePartitioning(ordering: Seq[SortOrder], numPartitions: Int)
  extends Expression with Partitioning with Unevaluable {

  override def children: Seq[SortOrder] = ordering
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def satisfies0(required: Distribution): Boolean = {
    super.satisfies0(required) || {
      required match {
        case OrderedDistribution(requiredOrdering) =>
          // If `ordering` is a prefix of `requiredOrdering`:
          //   Let's say `ordering` is [a, b] and `requiredOrdering` is [a, b, c]. According to the
          //   RangePartitioning definition, any [a, b] in a previous partition must be smaller
          //   than any [a, b] in the following partition. This also means any [a, b, c] in a
          //   previous partition must be smaller than any [a, b, c] in the following partition.
          //   Thus `RangePartitioning(a, b)` satisfies `OrderedDistribution(a, b, c)`.
          //
          // If `requiredOrdering` is a prefix of `ordering`:
          //   Let's say `ordering` is [a, b, c] and `requiredOrdering` is [a, b]. According to the
          //   RangePartitioning definition, any [a, b, c] in a previous partition must be smaller
          //   than any [a, b, c] in the following partition. If there is a [a1, b1] from a previous
          //   partition which is larger than a [a2, b2] from the following partition, then there
          //   must be a [a1, b1 c1] larger than [a2, b2, c2], which violates RangePartitioning
          //   definition. So it's guaranteed that, any [a, b] in a previous partition must not be
          //   greater(i.e. smaller or equal to) than any [a, b] in the following partition. Thus
          //   `RangePartitioning(a, b, c)` satisfies `OrderedDistribution(a, b)`.
          val minSize = Seq(requiredOrdering.size, ordering.size).min
          requiredOrdering.take(minSize) == ordering.take(minSize)
        case ClusteredDistribution(requiredClustering, _) =>
          ordering.map(_.child).forall(x => requiredClustering.exists(_.semanticEquals(x)))
        case _ => false
      }
    }
  }
}

/**
 * A collection of [[Partitioning]]s that can be used to describe the partitioning
 * scheme of the output of a physical operator. It is usually used for an operator
 * that has multiple children. In this case, a [[Partitioning]] in this collection
 * describes how this operator's output is partitioned based on expressions from
 * a child. For example, for a Join operator on two tables `A` and `B`
 * with a join condition `A.key1 = B.key2`, assuming we use HashPartitioning schema,
 * there are two [[Partitioning]]s can be used to describe how the output of
 * this Join operator is partitioned, which are `HashPartitioning(A.key1)` and
 * `HashPartitioning(B.key2)`. It is also worth noting that `partitionings`
 * in this collection do not need to be equivalent, which is useful for
 * Outer Join operators.
 */
case class PartitioningCollection(partitionings: Seq[Partitioning])
  extends Expression with Partitioning with Unevaluable {

  require(
    partitionings.map(_.numPartitions).distinct.length == 1,
    s"PartitioningCollection requires all of its partitionings have the same numPartitions.")

  override def children: Seq[Expression] = partitionings.collect {
    case expr: Expression => expr
  }

  override def nullable: Boolean = false

  override def dataType: DataType = IntegerType

  override val numPartitions = partitionings.map(_.numPartitions).distinct.head

  /**
   * Returns true if any `partitioning` of this collection satisfies the given
   * [[Distribution]].
   */
  override def satisfies0(required: Distribution): Boolean =
    partitionings.exists(_.satisfies(required))

  /**
   * Returns true if any `partitioning` of this collection is compatible with the given
   * [[Partitioning]]
   */
  override def isCompatibleWith(other: Partitioning): Boolean = {
    partitionings.exists(_.isCompatibleWith(other))
  }

  override def toString: String = {
    partitionings.map(_.toString).mkString("(", " or ", ")")
  }
}

/**
 * Represents a partitioning where rows are collected, transformed and broadcasted to each
 * node in the cluster.
 */
case class BroadcastPartitioning(mode: BroadcastMode) extends Partitioning {
  override val numPartitions: Int = 1

  override def satisfies0(required: Distribution): Boolean = required match {
    case BroadcastDistribution(m) if m == mode => true
    case _ => false
  }
}

/**
 * Represents a partitioning where rows are split across partitions based on transforms defined
 * by `expressions`. `partitionValues` should contain value of partition key(s) in ascending order,
 * after evaluated by the transforms in `expressions`, for each input partition. In addition, its
 * length must be the same as the number of input partitions (and thus is a 1-1 mapping), and each
 * row in `partitionValues` must be unique.
 *
 * For example, if `expressions` is `[years(ts_col)]`, then a valid value of `partitionValues` is
 * `[50, 51, 52]`, which represents 3 input partitions with distinct partition values. All rows
 * in each partition have the same value for column `ts_col` (which is of timestamp type), after
 * being applied by the `years` transform.
 *
 * On the other hand, `[50, 50, 51]` is not a valid value for `partitionValues` since `50` is
 * duplicated twice.
 *
 * - `expressions`: partition expressions for the partitioning.
 * - `partitionValues`: the values for the cluster keys of the distribution, must be in ascending
 *   order.
 */
case class DataSourcePartitioning(
    expressions: Seq[Expression],
    partitionValues: Seq[InternalRow]) extends Partitioning {
  override val numPartitions: Int = partitionValues.length

  override def satisfies0(required: physical.Distribution): Boolean = {
    super.satisfies0(required) || {
      required match {
        case p: HashClusteredDistribution =>
          val attributes = expressions.flatMap(DataSourcePartitioning.collectSingleExpression)
          p.expressions.length == attributes.length &&
            p.expressions.zip(attributes).forall {
              case (l, r) => l.semanticEquals(r)
            }
        case p: ClusteredDistribution =>
          val attributes = expressions.flatMap(DataSourcePartitioning.collectSingleExpression)
          attributes.forall(c => p.clustering.exists(_.semanticEquals(c)))
        case _ =>
          false
      }
    }
  }

  // TODO: we should consider distribution here together with partitioning, for instance:
  //   (bucket(32, col1), year(col2)) vs (bucket(32, col1), day(col3)) should be considered
  //   if join key is (col1). This will require us to group the buckets together which will
  //   introduce shuffle. We should introduce a planner rule to pass down the join keys down to
  //   scanner where the output partitioning is created.
  override def isCompatibleWith(partitioning: Partitioning): Boolean = partitioning match {
    case other: DataSourcePartitioning =>
      numPartitions == other.numPartitions &&
        DataSourcePartitioning.clusteringCompatible(expressions, other.expressions) &&
        partitionValues.zip(other.partitionValues).forall { case (v1, v2) =>
          v1 == v2
        }
    case PartitioningCollection(otherPartitionings) =>
      otherPartitionings.exists(_.isCompatibleWith(this))
    case _ =>
      false
  }
}

object DataSourcePartitioning {
  def clusteringCompatible(left: Seq[Expression], right: Seq[Expression]): Boolean = {
    left.length == right.length && left.zip(right).forall { case (l, r) =>
        expressionCompatible(l, r)
      }
  }

  // TODO: we should compare functions using their ID and argument length
  private def expressionCompatible(left: Expression, right: Expression): Boolean = {
    (left, right) match {
      case (_: Attribute, _: Attribute) => true
      case (_: Literal, _: Literal) => true
      case (_: IcebergYearTransform, _: IcebergYearTransform) => true
      case (_: IcebergMonthTransform, _: IcebergMonthTransform) => true
      case (_: IcebergDayTransform, _: IcebergDayTransform) => true
      case (_: IcebergHourTransform, _: IcebergHourTransform) => true
      case (IcebergBucketTransform(leftNumBuckets, _),
      IcebergBucketTransform(rightNumBuckets, _)) =>
        leftNumBuckets == rightNumBuckets
      case _ => false
    }
  }

  // TODO: make this tail recursive
  def collectSingleExpression(e: Expression): Seq[Expression] = e match {
    case IcebergYearTransform(child) => collectSingleExpression(child)
    case IcebergMonthTransform(child) => collectSingleExpression(child)
    case IcebergDayTransform(child) => collectSingleExpression(child)
    case IcebergHourTransform(child) => collectSingleExpression(child)
    case IcebergBucketTransform(_, children) => children.flatMap(collectSingleExpression)
    case a: Attribute => Seq(a)
    case l: Literal => Seq(l)
    case _ =>
      throw new IllegalArgumentException(s"unexpected expression: $e")
  }
}
