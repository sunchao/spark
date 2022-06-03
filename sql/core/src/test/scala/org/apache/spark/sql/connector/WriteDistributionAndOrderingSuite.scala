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

package org.apache.spark.sql.connector

import java.util.Collections

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{catalyst, AnalysisException, DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.plans.physical
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, RangePartitioning, UnknownPartitioning}
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryTableCatalog}
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference, NullOrdering, SortDirection, SortOrder}
import org.apache.spark.sql.connector.expressions.LogicalExpressions._
import org.apache.spark.sql.execution.{QueryExecution, SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.adaptive.AQEShuffleReadExec
import org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.test.SQLTestData.TestData
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.util.QueryExecutionListener

class WriteDistributionAndOrderingSuite
  extends QueryTest with SharedSparkSession with BeforeAndAfter with AdaptiveSparkPlanHelper {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
  import testImplicits._

  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.unsetConf("spark.sql.catalog.testcat")
  }

  private val namespace = Array("ns1")
  private val ident = Identifier.of(namespace, "test_table")
  private val tableNameAsString = "testcat." + ident.toString
  private val emptyProps = Collections.emptyMap[String, String]
  private val schema = new StructType()
    .add("id", IntegerType)
    .add("data", StringType)

  private val resolver = conf.resolver

  test("ordered distribution and sort with same exprs: append") {
    checkOrderedDistributionAndSortWithSameExprsInVariousCases("append")
  }

  test("ordered distribution and sort with same exprs: overwrite") {
    checkOrderedDistributionAndSortWithSameExprsInVariousCases("overwrite")
  }

  test("ordered distribution and sort with same exprs: overwriteDynamic") {
    checkOrderedDistributionAndSortWithSameExprsInVariousCases("overwriteDynamic")
  }

  test("ordered distribution and sort with same exprs with numPartitions: append") {
    checkOrderedDistributionAndSortWithSameExprs("append", Some(10))
  }

  test("ordered distribution and sort with same exprs with numPartitions: overwrite") {
    checkOrderedDistributionAndSortWithSameExprs("overwrite", Some(10))
  }

  test("ordered distribution and sort with same exprs with numPartitions: overwriteDynamic") {
    checkOrderedDistributionAndSortWithSameExprs("overwriteDynamic", Some(10))
  }

  private def checkOrderedDistributionAndSortWithSameExprsInVariousCases(cmd: String) = {
    Seq(true, false).foreach { distributionStrictlyRequired =>
      Seq(true, false).foreach { dataSkewed =>
        Seq(true, false).foreach { coalesce =>
          checkOrderedDistributionAndSortWithSameExprs(
            cmd, None, distributionStrictlyRequired, dataSkewed, coalesce)
        }
      }
    }
  }

  private def checkOrderedDistributionAndSortWithSameExprs(
      command: String,
      targetNumPartitions: Option[Int] = None,
      distributionStrictlyRequired: Boolean = true,
      dataSkewed: Boolean = false,
      coalesce: Boolean = false): Unit = {
    val tableOrdering = Array[SortOrder](
      sort(FieldReference("data"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    )
    val tableDistribution = Distributions.ordered(tableOrdering)

    val writeOrdering = Seq(
      catalyst.expressions.SortOrder(
        attr("data"),
        catalyst.expressions.Ascending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      )
    )
    val writePartitioning = if (!coalesce) {
      orderedWritePartitioning(writeOrdering, targetNumPartitions)
    } else {
      orderedWritePartitioning(writeOrdering, Some(1))
    }

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeCommand = command,
      distributionStrictlyRequired = distributionStrictlyRequired,
      dataSkewed = dataSkewed,
      coalesce = coalesce)
  }

  test("clustered distribution and sort with same exprs: append") {
    checkClusteredDistributionAndSortWithSameExprsInVariousCases("append")
  }

  test("clustered distribution and sort with same exprs: overwrite") {
    checkClusteredDistributionAndSortWithSameExprsInVariousCases("overwrite")
  }

  test("clustered distribution and sort with same exprs: overwriteDynamic") {
    checkClusteredDistributionAndSortWithSameExprsInVariousCases("overwriteDynamic")
  }

  test("clustered distribution and sort with same exprs with numPartitions: append") {
    checkClusteredDistributionAndSortWithSameExprs("append", Some(10))
  }

  test("clustered distribution and sort with same exprs with numPartitions: overwrite") {
    checkClusteredDistributionAndSortWithSameExprs("overwrite", Some(10))
  }

  test("clustered distribution and sort with same exprs with numPartitions: overwriteDynamic") {
    checkClusteredDistributionAndSortWithSameExprs("overwriteDynamic", Some(10))
  }

  private def checkClusteredDistributionAndSortWithSameExprsInVariousCases(cmd: String) = {
    Seq(true, false).foreach { distributionStrictlyRequired =>
      Seq(true, false).foreach { dataSkewed =>
        Seq(true, false).foreach { coalesce =>
          checkClusteredDistributionAndSortWithSameExprs(
            cmd, None, distributionStrictlyRequired, dataSkewed, coalesce)
        }
      }
    }
  }

  private def checkClusteredDistributionAndSortWithSameExprs(
      command: String,
      targetNumPartitions: Option[Int] = None,
      distributionStrictlyRequired: Boolean = true,
      dataSkewed: Boolean = false,
      coalesce: Boolean = false): Unit = {
    val tableOrdering = Array[SortOrder](
      sort(FieldReference("data"), SortDirection.DESCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    )
    val clustering = Array[Expression](FieldReference("data"), FieldReference("id"))
    val tableDistribution = Distributions.clustered(clustering)

    val writeOrdering = Seq(
      catalyst.expressions.SortOrder(
        attr("data"),
        catalyst.expressions.Descending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      ),
      catalyst.expressions.SortOrder(
        attr("id"),
        catalyst.expressions.Ascending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      )
    )
    val writePartitioningExprs = Seq(attr("data"), attr("id"))
    val writePartitioning = if (!coalesce) {
      clusteredWritePartitioning(writePartitioningExprs, targetNumPartitions)
    } else {
      clusteredWritePartitioning(writePartitioningExprs, Some(1))
    }

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeCommand = command,
      distributionStrictlyRequired = distributionStrictlyRequired,
      dataSkewed = dataSkewed,
      coalesce = coalesce)
  }

  test("clustered distribution and sort with extended exprs: append") {
    checkClusteredDistributionAndSortWithExtendedExprsInVariousCases("append")
  }

  test("clustered distribution and sort with extended exprs: overwrite") {
    checkClusteredDistributionAndSortWithExtendedExprsInVariousCases("overwrite")
  }

  test("clustered distribution and sort with extended exprs: overwriteDynamic") {
    checkClusteredDistributionAndSortWithExtendedExprsInVariousCases("overwriteDynamic")
  }

  test("clustered distribution and sort with extended exprs with numPartitions: append") {
    checkClusteredDistributionAndSortWithExtendedExprs("append", Some(10))
  }

  test("clustered distribution and sort with extended exprs with numPartitions: overwrite") {
    checkClusteredDistributionAndSortWithExtendedExprs("overwrite", Some(10))
  }

  test("clustered distribution and sort with extended exprs with numPartitions: " +
    "overwriteDynamic") {
    checkClusteredDistributionAndSortWithExtendedExprs("overwriteDynamic", Some(10))
  }

  private def checkClusteredDistributionAndSortWithExtendedExprsInVariousCases(cmd: String) = {
    Seq(true, false).foreach { distributionStrictlyRequired =>
      Seq(true, false).foreach { dataSkewed =>
        Seq(true, false).foreach { coalesce =>
          checkClusteredDistributionAndSortWithExtendedExprs(
            cmd, None, distributionStrictlyRequired, dataSkewed, coalesce)
        }
      }
    }
  }

  private def checkClusteredDistributionAndSortWithExtendedExprs(
      command: String,
      targetNumPartitions: Option[Int] = None,
      distributionStrictlyRequired: Boolean = true,
      dataSkewed: Boolean = false,
      coalesce: Boolean = false): Unit = {
    val tableOrdering = Array[SortOrder](
      sort(FieldReference("data"), SortDirection.DESCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    )
    val clustering = Array[Expression](FieldReference("data"))
    val tableDistribution = Distributions.clustered(clustering)

    val writeOrdering = Seq(
      catalyst.expressions.SortOrder(
        attr("data"),
        catalyst.expressions.Descending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      ),
      catalyst.expressions.SortOrder(
        attr("id"),
        catalyst.expressions.Ascending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      )
    )
    val writePartitioningExprs = Seq(attr("data"))
    val writePartitioning = if (!coalesce) {
      clusteredWritePartitioning(writePartitioningExprs, targetNumPartitions)
    } else {
      clusteredWritePartitioning(writePartitioningExprs, Some(1))
    }

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeCommand = command,
      distributionStrictlyRequired = distributionStrictlyRequired,
      dataSkewed = dataSkewed,
      coalesce = coalesce)
  }

  test("unspecified distribution and local sort: append") {
    checkUnspecifiedDistributionAndLocalSort("append")
  }

  test("unspecified distribution and local sort: overwrite") {
    checkUnspecifiedDistributionAndLocalSort("overwrite")
  }

  test("unspecified distribution and local sort: overwriteDynamic") {
    checkUnspecifiedDistributionAndLocalSort("overwriteDynamic")
  }

  test("unspecified distribution and local sort with numPartitions: append") {
    checkUnspecifiedDistributionAndLocalSort("append", Some(10))
  }

  test("unspecified distribution and local sort with numPartitions: overwrite") {
    checkUnspecifiedDistributionAndLocalSort("overwrite", Some(10))
  }

  test("unspecified distribution and local sort with numPartitions: overwriteDynamic") {
    checkUnspecifiedDistributionAndLocalSort("overwriteDynamic", Some(10))
  }

  private def checkUnspecifiedDistributionAndLocalSort(command: String): Unit = {
    checkUnspecifiedDistributionAndLocalSort(command, None)
  }

  private def checkUnspecifiedDistributionAndLocalSort(
      command: String,
      targetNumPartitions: Option[Int]): Unit = {
    val tableOrdering = Array[SortOrder](
      sort(FieldReference("data"), SortDirection.DESCENDING, NullOrdering.NULLS_FIRST)
    )
    val tableDistribution = Distributions.unspecified()

    val writeOrdering = Seq(
      catalyst.expressions.SortOrder(
        attr("data"),
        catalyst.expressions.Descending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      )
    )

    val writePartitioning = UnknownPartitioning(0)

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeCommand = command,
      // if the number of partitions is specified, we expect query to fail
      expectAnalysisException = targetNumPartitions.isDefined)
  }

  test("unspecified distribution and no sort: append") {
    checkUnspecifiedDistributionAndNoSort("append")
  }

  test("unspecified distribution and no sort: overwrite") {
    checkUnspecifiedDistributionAndNoSort("overwrite")
  }

  test("unspecified distribution and no sort: overwriteDynamic") {
    checkUnspecifiedDistributionAndNoSort("overwriteDynamic")
  }

  test("unspecified distribution and no sort with numPartitions: append") {
    checkUnspecifiedDistributionAndNoSort("append", Some(10))
  }

  test("unspecified distribution and no sort with numPartitions: overwrite") {
    checkUnspecifiedDistributionAndNoSort("overwrite", Some(10))
  }

  test("unspecified distribution and no sort with numPartitions: overwriteDynamic") {
    checkUnspecifiedDistributionAndNoSort("overwriteDynamic", Some(10))
  }

  private def checkUnspecifiedDistributionAndNoSort(command: String): Unit = {
    checkUnspecifiedDistributionAndNoSort(command, None)
  }

  private def checkUnspecifiedDistributionAndNoSort(
      command: String,
      targetNumPartitions: Option[Int]): Unit = {
    val tableOrdering = Array.empty[SortOrder]
    val tableDistribution = Distributions.unspecified()

    val writeOrdering = Seq.empty[catalyst.expressions.SortOrder]
    val writePartitioning = UnknownPartitioning(0)

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeCommand = command,
      // if the number of partitions is specified, we expect query to fail
      expectAnalysisException = targetNumPartitions.isDefined)
  }

  test("ordered distribution and sort with manual global sort: append") {
    checkOrderedDistributionAndSortWithManualGlobalSortInVariousCases("append")
  }

  test("ordered distribution and sort with manual global sort: overwrite") {
    checkOrderedDistributionAndSortWithManualGlobalSortInVariousCases("overwrite")
  }

  test("ordered distribution and sort with manual global sort: overwriteDynamic") {
    checkOrderedDistributionAndSortWithManualGlobalSortInVariousCases("overwriteDynamic")
  }

  test("ordered distribution and sort with manual global sort with numPartitions: append") {
    checkOrderedDistributionAndSortWithManualGlobalSort("append", Some(10))
  }

  test("ordered distribution and sort with manual global sort with numPartitions: overwrite") {
    checkOrderedDistributionAndSortWithManualGlobalSort("overwrite", Some(10))
  }

  test("ordered distribution and sort with manual global sort with numPartitions: " +
    "overwriteDynamic") {
    checkOrderedDistributionAndSortWithManualGlobalSort("overwriteDynamic", Some(10))
  }

  private def checkOrderedDistributionAndSortWithManualGlobalSortInVariousCases(cmd: String) = {
    Seq(true, false).foreach { distributionStrictlyRequired =>
      Seq(true, false).foreach { dataSkewed =>
        Seq(true, false).foreach { coalesce =>
          checkOrderedDistributionAndSortWithManualGlobalSort(
            cmd, None, distributionStrictlyRequired, dataSkewed, coalesce)
        }
      }
    }
  }

  private def checkOrderedDistributionAndSortWithManualGlobalSort(
      command: String,
      targetNumPartitions: Option[Int] = None,
      distributionStrictlyRequired: Boolean = true,
      dataSkewed: Boolean = false,
      coalesce: Boolean = false): Unit = {
    val tableOrdering = Array[SortOrder](
      sort(FieldReference("data"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    )
    val tableDistribution = Distributions.ordered(tableOrdering)

    val writeOrdering = Seq(
      catalyst.expressions.SortOrder(
        attr("data"),
        catalyst.expressions.Ascending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      ),
      catalyst.expressions.SortOrder(
        attr("id"),
        catalyst.expressions.Ascending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      )
    )
    val writePartitioning = if (!coalesce) {
      orderedWritePartitioning(writeOrdering, targetNumPartitions)
    } else {
      orderedWritePartitioning(writeOrdering, Some(1))
    }

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeTransform = df => df.orderBy("data", "id"),
      writeCommand = command,
      distributionStrictlyRequired = distributionStrictlyRequired,
      dataSkewed = dataSkewed,
      coalesce = coalesce)
  }

  test("ordered distribution and sort with incompatible global sort: append") {
    checkOrderedDistributionAndSortWithIncompatibleGlobalSortInVariousCases("append")
  }

  test("ordered distribution and sort with incompatible global sort: overwrite") {
    checkOrderedDistributionAndSortWithIncompatibleGlobalSortInVariousCases("overwrite")
  }

  test("ordered distribution and sort with incompatible global sort: overwriteDynamic") {
    checkOrderedDistributionAndSortWithIncompatibleGlobalSortInVariousCases("overwriteDynamic")
  }

  test("ordered distribution and sort with incompatible global sort with numPartitions: append") {
    checkOrderedDistributionAndSortWithIncompatibleGlobalSort("append", Some(10))
  }

  test("ordered distribution and sort with incompatible global sort with numPartitions: " +
    "overwrite") {
    checkOrderedDistributionAndSortWithIncompatibleGlobalSort("overwrite", Some(10))
  }

  test("ordered distribution and sort with incompatible global sort with numPartitions: " +
    "overwriteDynamic") {
    checkOrderedDistributionAndSortWithIncompatibleGlobalSort("overwriteDynamic", Some(10))
  }

  private def checkOrderedDistributionAndSortWithIncompatibleGlobalSortInVariousCases(
      cmd: String) = {
    Seq(true, false).foreach { distributionStrictlyRequired =>
      Seq(true, false).foreach { dataSkewed =>
        Seq(true, false).foreach { coalesce =>
          checkOrderedDistributionAndSortWithIncompatibleGlobalSort(
            cmd, None, distributionStrictlyRequired, dataSkewed, coalesce)
        }
      }
    }
  }

  private def checkOrderedDistributionAndSortWithIncompatibleGlobalSort(
      command: String,
      targetNumPartitions: Option[Int] = None,
      distributionStrictlyRequired: Boolean = true,
      dataSkewed: Boolean = false,
      coalesce: Boolean = false): Unit = {
    val tableOrdering = Array[SortOrder](
      sort(FieldReference("data"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    )
    val tableDistribution = Distributions.ordered(tableOrdering)

    val writeOrdering = Seq(
      catalyst.expressions.SortOrder(
        attr("data"),
        catalyst.expressions.Ascending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      ),
      catalyst.expressions.SortOrder(
        attr("id"),
        catalyst.expressions.Ascending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      )
    )
    val writePartitioning = if (!coalesce) {
      orderedWritePartitioning(writeOrdering, targetNumPartitions)
    } else {
      orderedWritePartitioning(writeOrdering, Some(1))
    }

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeTransform = df => df.orderBy(df("data").desc, df("id").asc),
      writeCommand = command,
      distributionStrictlyRequired = distributionStrictlyRequired,
      dataSkewed = dataSkewed,
      coalesce = coalesce)
  }

  test("ordered distribution and sort with manual local sort: append") {
    checkOrderedDistributionAndSortWithManualLocalSortInVariousCases("append")
  }

  test("ordered distribution and sort with manual local sort: overwrite") {
    checkOrderedDistributionAndSortWithManualLocalSortInVariousCases("overwrite")
  }

  test("ordered distribution and sort with manual local sort: overwriteDynamic") {
    checkOrderedDistributionAndSortWithManualLocalSortInVariousCases("overwriteDynamic")
  }

  test("ordered distribution and sort with manual local sort with numPartitions: append") {
    checkOrderedDistributionAndSortWithManualLocalSort("append", Some(10))
  }

  test("ordered distribution and sort with manual local sort with numPartitions: overwrite") {
    checkOrderedDistributionAndSortWithManualLocalSort("overwrite", Some(10))
  }

  test("ordered distribution and sort with manual local sort with numPartitions: " +
    "overwriteDynamic") {
    checkOrderedDistributionAndSortWithManualLocalSort("overwriteDynamic", Some(10))
  }

  private def checkOrderedDistributionAndSortWithManualLocalSortInVariousCases(cmd: String) = {
    Seq(true, false).foreach { distributionStrictlyRequired =>
      Seq(true, false).foreach { dataSkewed =>
        Seq(true, false).foreach { coalesce =>
          checkOrderedDistributionAndSortWithManualLocalSort(
            cmd, None, distributionStrictlyRequired, dataSkewed, coalesce)
        }
      }
    }
  }

  private def checkOrderedDistributionAndSortWithManualLocalSort(
      command: String,
      targetNumPartitions: Option[Int] = None,
      distributionStrictlyRequired: Boolean = true,
      dataSkewed: Boolean = false,
      coalesce: Boolean = false): Unit = {
    val tableOrdering = Array[SortOrder](
      sort(FieldReference("data"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    )
    val tableDistribution = Distributions.ordered(tableOrdering)

    val writeOrdering = Seq(
      catalyst.expressions.SortOrder(
        attr("data"),
        catalyst.expressions.Ascending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      ),
      catalyst.expressions.SortOrder(
        attr("id"),
        catalyst.expressions.Ascending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      )
    )
    val writePartitioning = if (!coalesce) {
      orderedWritePartitioning(writeOrdering, targetNumPartitions)
    } else {
      orderedWritePartitioning(writeOrdering, Some(1))
    }

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeTransform = df => df.sortWithinPartitions("data", "id"),
      writeCommand = command,
      distributionStrictlyRequired = distributionStrictlyRequired,
      dataSkewed = dataSkewed,
      coalesce = coalesce)
  }

  test("clustered distribution and local sort with manual global sort: append") {
    checkClusteredDistributionAndLocalSortWithManualGlobalSortInVariousCases("append")
  }

  test("clustered distribution and local sort with manual global sort: overwrite") {
    checkClusteredDistributionAndLocalSortWithManualGlobalSortInVariousCases("overwrite")
  }

  test("clustered distribution and local sort with manual global sort: overwriteDynamic") {
    checkClusteredDistributionAndLocalSortWithManualGlobalSortInVariousCases("overwriteDynamic")
  }

  test("clustered distribution and local sort with manual global sort with numPartitions: append") {
    checkClusteredDistributionAndLocalSortWithManualGlobalSort("append", Some(10))
  }

  test("clustered distribution and local sort with manual global sort with numPartitions: " +
    "overwrite") {
    checkClusteredDistributionAndLocalSortWithManualGlobalSort("overwrite", Some(10))
  }

  test("clustered distribution and local sort with manual global sort with numPartitions: " +
    "overwriteDynamic") {
    checkClusteredDistributionAndLocalSortWithManualGlobalSort("overwriteDynamic", Some(10))
  }

  private def checkClusteredDistributionAndLocalSortWithManualGlobalSortInVariousCases(
      cmd: String) = {
    Seq(true, false).foreach { distributionStrictlyRequired =>
      Seq(true, false).foreach { dataSkewed =>
        Seq(true, false).foreach { coalesce =>
          checkClusteredDistributionAndLocalSortWithManualGlobalSort(
            cmd, None, distributionStrictlyRequired, dataSkewed, coalesce)
        }
      }
    }
  }

  private def checkClusteredDistributionAndLocalSortWithManualGlobalSort(
      command: String,
      targetNumPartitions: Option[Int] = None,
      distributionStrictlyRequired: Boolean = true,
      dataSkewed: Boolean = false,
      coalesce: Boolean = false): Unit = {
    val tableOrdering = Array[SortOrder](
      sort(FieldReference("data"), SortDirection.DESCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    )
    val tableDistribution = Distributions.clustered(Array(FieldReference("data")))

    val writeOrdering = Seq(
      catalyst.expressions.SortOrder(
        attr("data"),
        catalyst.expressions.Descending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      ),
      catalyst.expressions.SortOrder(
        attr("id"),
        catalyst.expressions.Ascending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      )
    )
    val writePartitioningExprs = Seq(attr("data"))
    val writePartitioning = if (!coalesce) {
      clusteredWritePartitioning(writePartitioningExprs, targetNumPartitions)
    } else {
      clusteredWritePartitioning(writePartitioningExprs, Some(1))
    }

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeTransform = df => df.orderBy("data", "id"),
      writeCommand = command,
      distributionStrictlyRequired = distributionStrictlyRequired,
      dataSkewed = dataSkewed,
      coalesce = coalesce)
  }

  test("clustered distribution and local sort with manual local sort: append") {
    checkClusteredDistributionAndLocalSortWithManualLocalSortInVariousCases("append")
  }

  test("clustered distribution and local sort with manual local sort: overwrite") {
    checkClusteredDistributionAndLocalSortWithManualLocalSortInVariousCases("overwrite")
  }

  test("clustered distribution and local sort with manual local sort: overwriteDynamic") {
    checkClusteredDistributionAndLocalSortWithManualLocalSortInVariousCases("overwriteDynamic")
  }

  test("clustered distribution and local sort with manual local sort with numPartitions: append") {
    checkClusteredDistributionAndLocalSortWithManualLocalSort("append", Some(10))
  }

  test("clustered distribution and local sort with manual local sort with numPartitions: " +
    "overwrite") {
    checkClusteredDistributionAndLocalSortWithManualLocalSort("overwrite", Some(10))
  }

  test("clustered distribution and local sort with manual local sort with numPartitions: " +
    "overwriteDynamic") {
    checkClusteredDistributionAndLocalSortWithManualLocalSort("overwriteDynamic", Some(10))
  }

  private def checkClusteredDistributionAndLocalSortWithManualLocalSortInVariousCases(
      cmd: String) = {
    Seq(true, false).foreach { distributionStrictlyRequired =>
      Seq(true, false).foreach { dataSkewed =>
        Seq(true, false).foreach { coalesce =>
          checkClusteredDistributionAndLocalSortWithManualLocalSort(
            cmd, None, distributionStrictlyRequired, dataSkewed, coalesce)
        }
      }
    }
  }

  private def checkClusteredDistributionAndLocalSortWithManualLocalSort(
      command: String,
      targetNumPartitions: Option[Int] = None,
      distributionStrictlyRequired: Boolean = true,
      dataSkewed: Boolean = false,
      coalesce: Boolean = false): Unit = {
    val tableOrdering = Array[SortOrder](
      sort(FieldReference("data"), SortDirection.DESCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    )
    val tableDistribution = Distributions.clustered(Array(FieldReference("data")))

    val writeOrdering = Seq(
      catalyst.expressions.SortOrder(
        attr("data"),
        catalyst.expressions.Descending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      ),
      catalyst.expressions.SortOrder(
        attr("id"),
        catalyst.expressions.Ascending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      )
    )
    val writePartitioningExprs = Seq(attr("data"))
    val writePartitioning = if (!coalesce) {
      clusteredWritePartitioning(writePartitioningExprs, targetNumPartitions)
    } else {
      clusteredWritePartitioning(writePartitioningExprs, Some(1))
    }

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeTransform = df => df.orderBy("data", "id"),
      writeCommand = command,
      distributionStrictlyRequired = distributionStrictlyRequired,
      dataSkewed = dataSkewed,
      coalesce = coalesce)
  }

  // scalastyle:off argcount
  private def checkWriteRequirements(
      tableDistribution: Distribution,
      tableOrdering: Array[SortOrder],
      tableNumPartitions: Option[Int],
      expectedWritePartitioning: physical.Partitioning,
      expectedWriteOrdering: Seq[catalyst.expressions.SortOrder],
      writeTransform: DataFrame => DataFrame = df => df,
      writeCommand: String = "append",
      expectAnalysisException: Boolean = false,
      distributionStrictlyRequired: Boolean = true,
      dataSkewed: Boolean = false,
      coalesce: Boolean = false): Unit = {
    // scalastyle:on argcount

    catalog.createTableWithDistributionAndOrdering(
      ident, schema, Array.empty, emptyProps, tableDistribution,
      tableOrdering, tableNumPartitions, distributionStrictlyRequired)

    val df = if (!dataSkewed) {
      spark.createDataFrame(Seq((1, "a"), (2, "b"), (3, "c"))).toDF("id", "data")
    } else {
      spark.sparkContext.parallelize(
        (1 to 10).map(i => TestData(if (i > 4) 5 else i, i.toString)), 3)
        .toDF("id", "data")
    }
    val writer = writeTransform(df).writeTo(tableNameAsString)

    def executeCommand(): SparkPlan = writeCommand match {
      case "append" => execute(writer.append())
      case "overwrite" => execute(writer.overwrite(lit(true)))
      case "overwriteDynamic" => execute(writer.overwritePartitions())
    }

    if (expectAnalysisException) {
      intercept[AnalysisException] {
        executeCommand()
      }
    } else {
      if (coalesce) {
        val executedPlan = executeCommand()
        val read = collect(executedPlan) {
          case r: AQEShuffleReadExec => r
        }
        assert(read.size == 1)
        assert(read.head.partitionSpecs.size == 1)
        checkPartitioningAndOrdering(
          // num of partition in expectedWritePartitioning is 1
          executedPlan, expectedWritePartitioning, expectedWriteOrdering)
      } else {
        if (dataSkewed && !distributionStrictlyRequired) {
          withSQLConf(
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
            SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
            SQLConf.ADAPTIVE_OPTIMIZE_SKEWS_IN_REBALANCE_PARTITIONS_ENABLED.key -> "true",
            SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
            SQLConf.SHUFFLE_PARTITIONS.key -> "5",
            SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "50",
            SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1") {
            val executedPlan = executeCommand()
            val read = collect(executedPlan) {
              case r: AQEShuffleReadExec => r
            }
            assert(read.size == 1)
            assert(read.head.partitionSpecs.size >= 7)

            checkPartitioningAndOrdering(
              executedPlan, expectedWritePartitioning, expectedWriteOrdering, true)
          }
        } else {
          withSQLConf(
            SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false") {
            val executedPlan = executeCommand()
            checkPartitioningAndOrdering(
              executedPlan, expectedWritePartitioning, expectedWriteOrdering)
          }
        }
      }
      checkAnswer(spark.table(tableNameAsString), df)
    }
    catalog.dropTable(ident)
  }

  private def checkPartitioningAndOrdering(
      plan: SparkPlan,
      partitioning: physical.Partitioning,
      ordering: Seq[catalyst.expressions.SortOrder],
      skewSplit: Boolean = false): Unit = {

    val sorts = collect(plan) { case s: SortExec => s }
    assert(sorts.size <= 1, "must be at most one sort")
    val shuffles = collect(plan) { case s: ShuffleExchangeLike => s }
    assert(shuffles.size <= 1, "must be at most one shuffle")

    val actualPartitioning = plan.outputPartitioning
    val expectedPartitioning = partitioning match {
      case p: physical.RangePartitioning =>
        val resolvedOrdering = p.ordering.map(resolveAttrs(_, plan))
        p.copy(ordering = resolvedOrdering.asInstanceOf[Seq[catalyst.expressions.SortOrder]])
      case p: physical.HashPartitioning =>
        val resolvedExprs = p.expressions.map(resolveAttrs(_, plan))
        p.copy(expressions = resolvedExprs)
      case other => other
    }
    if (skewSplit) {
      assert(actualPartitioning.numPartitions > conf.numShufflePartitions)
    } else {
      assert(actualPartitioning == expectedPartitioning, "partitioning must match")
    }
    val actualOrdering = plan.outputOrdering
    val expectedOrdering = ordering.map(resolveAttrs(_, plan))
    assert(actualOrdering == expectedOrdering, "ordering must match")
  }

  private def resolveAttrs(
      expr: catalyst.expressions.Expression,
      plan: SparkPlan): catalyst.expressions.Expression = {

    expr.transform {
      case UnresolvedAttribute(Seq(attrName)) =>
        plan.output.find(attr => resolver(attr.name, attrName)).get
      case UnresolvedAttribute(nameParts) =>
        val attrName = nameParts.mkString(".")
        fail(s"cannot resolve a nested attr: $attrName")
    }
  }

  private def attr(name: String): UnresolvedAttribute = {
    UnresolvedAttribute(name)
  }

  private def catalog: InMemoryTableCatalog = {
    val catalog = spark.sessionState.catalogManager.catalog("testcat")
    catalog.asTableCatalog.asInstanceOf[InMemoryTableCatalog]
  }

  // executes a write operation and keeps the executed physical plan
  private def execute(writeFunc: => Unit): SparkPlan = {
    var executedPlan: SparkPlan = null

    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        executedPlan = qe.executedPlan
      }
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
      }
    }
    spark.listenerManager.register(listener)

    writeFunc

    sparkContext.listenerBus.waitUntilEmpty()

    executedPlan match {
      case w: V2TableWriteExec =>
        stripAQEPlan(w.query)
      case _ =>
        fail("expected V2TableWriteExec")
    }
  }

  private def orderedWritePartitioning(
      writeOrdering: Seq[catalyst.expressions.SortOrder],
      targetNumPartitions: Option[Int]): physical.Partitioning = {
    RangePartitioning(writeOrdering, targetNumPartitions.getOrElse(conf.numShufflePartitions))
  }

  private def clusteredWritePartitioning(
      writePartitioningExprs: Seq[catalyst.expressions.Expression],
      targetNumPartitions: Option[Int]): physical.Partitioning = {
    HashPartitioning(writePartitioningExprs,
      targetNumPartitions.getOrElse(conf.numShufflePartitions))
  }
}
