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

import java.util
import java.util.Collections

import scala.collection.JavaConverters._

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, IcebergBucketTransform, IcebergYearTransform, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical
import org.apache.spark.sql.catalyst.plans.physical.{DataSourcePartitioning, Partitioning, SinglePartition, UnknownPartitioning}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ
import org.apache.spark.sql.connector.distributions.{Distribution => V2Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{Expression => V2Expression, Expressions, FieldReference, NullOrdering, SortDirection, SortOrder => V2SortOrder, Transform}
import org.apache.spark.sql.connector.expressions.Expressions._
import org.apache.spark.sql.connector.read.{Batch, HasPartitionKey, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder, SupportsReportPartitioning}
import org.apache.spark.sql.connector.read.partitioning.{Partitioning => V2Partitioning}
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanRelation}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

/**
 * Test suite for data sources report partitioning to Spark.
 *
 * TODO: test outer joins, broadcast joins etc.
 */
class DataSourceV2PartitioningSuite extends DistributionAndOrderingBase {
  private val emptyProps: util.Map[String, String] = Collections.emptyMap[String, String]
  private val table: String = "tbl"
  private val schema = new StructType()
    .add("id", LongType)
    .add("data", StringType)
    .add("ts", TimestampType)

  test("clustered distribution: output partitioning should be DataSourcePartitioning") {
    val partitions: Array[Transform] = Array(Expressions.years("ts"))

    // create a table with 3 partitions, partitioned by `years` transform
    createTable(table, schema, partitions,
      Distributions.clustered(partitions.map(_.asInstanceOf[V2Expression])))
    sql(s"INSERT INTO testcat.ns.$table VALUES " +
        s"(0, 'aaa', CAST('2022-01-01' AS timestamp)), " +
        s"(1, 'bbb', CAST('2021-01-01' AS timestamp)), " +
        s"(2, 'ccc', CAST('2020-01-01' AS timestamp))")

    val df = sql(s"SELECT * FROM testcat.ns.$table")
    val distribution = physical.ClusteredDistribution(Seq(IcebergYearTransform(attr("ts"))))
    val partitionValues = Seq(50, 51, 52).map(v => InternalRow.fromSeq(Seq(v)))

    checkQueryPlan(df, distribution, Seq.empty,
      DataSourcePartitioning(distribution.clustering, partitionValues))
  }

  test("non-clustered distribution: fallback to super.partitioning") {
    val partitions: Array[Transform] = Array(years("ts"))
    val v2Ordering: Array[V2SortOrder] = Array(sort(FieldReference("ts"),
      SortDirection.ASCENDING, NullOrdering.NULLS_FIRST))

    createTable(table, schema, partitions, Distributions.ordered(v2Ordering), v2Ordering)
    sql(s"INSERT INTO testcat.ns.$table VALUES " +
      s"(0, 'aaa', CAST('2022-01-01' AS timestamp)), " +
      s"(1, 'bbb', CAST('2021-01-01' AS timestamp)), " +
      s"(2, 'ccc', CAST('2020-01-01' AS timestamp))")

    val df = sql(s"SELECT * FROM testcat.ns.$table")
    val ordering = Seq(SortOrder(attr("ts"), Ascending))
    val distribution = physical.OrderedDistribution(ordering)

    checkQueryPlan(df, distribution, ordering, UnknownPartitioning(0))
  }

  test("non-clustered distribution: no partition") {
    val partitions: Array[Transform] = Array(bucket(32, "ts"))
    createTable(table, schema, partitions,
      Distributions.clustered(partitions.map(_.asInstanceOf[V2Expression])))

    val df = sql(s"SELECT * FROM testcat.ns.$table")
    val distribution = physical.ClusteredDistribution(Seq(IcebergBucketTransform(32, attr("ts"))))

    checkQueryPlan(df, distribution, Seq.empty, UnknownPartitioning(0))
  }

  test("non-clustered distribution: single partition") {
    val partitions: Array[Transform] = Array(bucket(32, "ts"))
    createTable(table, schema, partitions,
      Distributions.clustered(partitions.map(_.asInstanceOf[V2Expression])))
    sql(s"INSERT INTO testcat.ns.$table VALUES (0, 'aaa', CAST('2020-01-01' AS timestamp))")

    val df = sql(s"SELECT * FROM testcat.ns.$table")
    val distribution = physical.ClusteredDistribution(Seq(IcebergBucketTransform(32, attr("ts"))))

    checkQueryPlan(df, distribution, Seq.empty, SinglePartition)
  }

  test("non-clustered distribution: not all partitions implement HasPartitionKey") {
    val df = spark.read.format(classOf[PartitionedDataSourceV2].getName).load()
    val distribution = physical.ClusteredDistribution(
      Seq(IcebergBucketTransform(32, attr("part_col1")), IcebergYearTransform(attr("part_col2"))))

    checkQueryPlan(df, distribution, Seq.empty, UnknownPartitioning(0))
  }

  private val customers: String = "customers"
  private val customers_schema = new StructType()
    .add("customer_name", StringType)
    .add("customer_age", IntegerType)
    .add("customer_id", LongType)

  private val orders: String = "orders"
  private val orders_schema = new StructType()
    .add("order_amount", DoubleType)
    .add("customer_id", LongType)

  private def testWithCustomersAndOrders(
      customers_partitions: Array[Transform],
      customers_distribution: V2Distribution,
      orders_partitions: Array[Transform],
      orders_distribution: V2Distribution,
      expectedNumOfShuffleExecs: Int): Unit = {
    createTable(customers, customers_schema, customers_partitions, customers_distribution)
    sql(s"INSERT INTO testcat.ns.$customers VALUES " +
      s"('aaa', 10, 1), ('bbb', 20, 2), ('ccc', 30, 3)")

    createTable(orders, orders_schema, orders_partitions, orders_distribution)
    sql(s"INSERT INTO testcat.ns.$orders VALUES " +
      s"(100.0, 1), (200.0, 1), (150.0, 2), (250.0, 2), (350.0, 2), (400.50, 3)")

    val df = sql("SELECT customer_name, customer_age, order_amount " +
      s"FROM testcat.ns.$customers c JOIN testcat.ns.$orders o " +
      "ON c.customer_id = o.customer_id ORDER BY c.customer_id, order_amount")

    val shuffles = collect(df.queryExecution.executedPlan) {
      case s: ShuffleExchangeExec => s
    }
    // this includes a shuffle from the ORDER BY clause
    assert(shuffles.length == expectedNumOfShuffleExecs)

    checkAnswer(df,
      Seq(Row("aaa", 10, 100.0), Row("aaa", 10, 200.0), Row("bbb", 20, 150.0),
        Row("bbb", 20, 250.0), Row("bbb", 20, 350.0), Row("ccc", 30, 400.50)))
  }

  test("partitioned join: exact distribution (same number of buckets) from both sides") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val customers_partitions = Array(bucket(4, "customer_id"))
      val orders_partitions = Array(bucket(4, "customer_id"))

      testWithCustomersAndOrders(customers_partitions,
        Distributions.clustered(customers_partitions.toArray),
        orders_partitions,
        Distributions.clustered(orders_partitions.toArray),
        1)
    }
  }

  test("partitioned join: number of buckets mismatch should trigger shuffle") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val customers_partitions = Array(bucket(4, "customer_id"))
      val orders_partitions = Array(bucket(2, "customer_id"))

      testWithCustomersAndOrders(customers_partitions,
        Distributions.clustered(customers_partitions.toArray),
        orders_partitions,
        Distributions.clustered(orders_partitions.toArray),
        3)
    }
  }

  test("partitioned join: only one side reports partitioning") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val customers_partitions = Array(bucket(4, "customer_id"))
      val orders_partitions = Array(bucket(2, "customer_id"))

      testWithCustomersAndOrders(customers_partitions,
        Distributions.clustered(customers_partitions.toArray),
        orders_partitions,
        Distributions.unspecified(),
        3)
    }
  }

  private val items: String = "items"
  private val items_schema: StructType = new StructType()
    .add("id", LongType)
    .add("name", StringType)
    .add("price", FloatType)
    .add("arrive_time", TimestampType)

  private val purchases: String = "purchases"
  private val purchases_schema: StructType = new StructType()
    .add("item_id", LongType)
    .add("price", FloatType)
    .add("time", TimestampType)

  test("partitioned join: join with two partition keys and matching partitions") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val items_partitions = Array(bucket(4, "id"), days("arrive_time"))
      createTable(items, items_schema, items_partitions,
        Distributions.clustered(items_partitions.toArray))
      sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.5, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

      val purchases_partitions = Array(bucket(4, "item_id"), days("time"))
      createTable(purchases, purchases_schema, purchases_partitions,
        Distributions.clustered(purchases_partitions.toArray))
      sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 44.0, cast('2020-01-15' as timestamp)), " +
        s"(1, 45.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 19.5, cast('2020-02-01' as timestamp))")

      val df = sql("SELECT id, name, i.price as purchase_price, p.price as sale_price " +
        s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
        "ON i.id = p.item_id AND i.arrive_time = p.time ORDER BY id, purchase_price, sale_price")

      val shuffles = collect(df.queryExecution.executedPlan) {
        case s: ShuffleExchangeExec => s
      }
      // this includes a shuffle from the ORDER BY clause
      assert(shuffles.length == 1, "should not add shuffle for both sides of the join")
      checkAnswer(df,
        Seq(Row(1, "aa", 40.0, 42.0), Row(1, "aa", 41.0, 44.0), Row(1, "aa", 41.0, 45.0),
          Row(2, "bb", 10.0, 11.0), Row(2, "bb", 10.5, 11.0), Row(3, "cc", 15.5, 19.5))
      )
    }
  }

  test("partitioned join: join with two partition keys but different # of partitions") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val items_partitions = Array(bucket(4, "id"), days("arrive_time"))
      createTable(items, items_schema, items_partitions,
        Distributions.clustered(items_partitions.toArray))
      sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.5, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp)), " +
        s"(3, 'cc', 16.0, cast('2020-03-01' as timestamp))")

      val purchases_partitions = Array(bucket(4, "item_id"), days("time"))
      createTable(purchases, purchases_schema, purchases_partitions,
        Distributions.clustered(purchases_partitions.toArray))
      sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 44.0, cast('2020-01-15' as timestamp)), " +
        s"(1, 45.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 19.5, cast('2020-02-01' as timestamp))")

      val df = sql("SELECT id, name, i.price as purchase_price, p.price as sale_price " +
        s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
        "ON i.id = p.item_id AND i.arrive_time = p.time ORDER BY id, purchase_price, sale_price")

      val shuffles = collect(df.queryExecution.executedPlan) {
        case s: ShuffleExchangeExec => s
      }
      // this includes a shuffle from the ORDER BY clause
      assert(shuffles.length == 3, "should not add shuffle for both sides of the join")
      checkAnswer(df,
        Seq(Row(1, "aa", 40.0, 42.0), Row(1, "aa", 41.0, 44.0), Row(1, "aa", 41.0, 45.0),
          Row(2, "bb", 10.0, 11.0), Row(2, "bb", 10.5, 11.0), Row(3, "cc", 15.5, 19.5))
      )
    }
  }

  test("partitioned join: join with two partition keys but different partitions") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val items_partitions = Array(bucket(4, "id"), days("arrive_time"))
      createTable(items, items_schema, items_partitions,
        Distributions.clustered(items_partitions.toArray))
      sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.5, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-03-01' as timestamp))")

      val purchases_partitions = Array(bucket(4, "item_id"), days("time"))
      createTable(purchases, purchases_schema, purchases_partitions,
        Distributions.clustered(purchases_partitions.toArray))
      sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 44.0, cast('2020-01-15' as timestamp)), " +
        s"(1, 45.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 19.5, cast('2020-02-01' as timestamp))")

      val df = sql("SELECT id, name, i.price as purchase_price, p.price as sale_price " +
        s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
        "ON i.id = p.item_id AND i.arrive_time = p.time ORDER BY id, purchase_price, sale_price")

      val shuffles = collect(df.queryExecution.executedPlan) {
        case s: ShuffleExchangeExec => s
      }
      // this includes a shuffle from the ORDER BY clause
      assert(shuffles.length == 3, "should not add shuffle for both sides of the join")
      checkAnswer(df,
        Seq(Row(1, "aa", 40.0, 42.0), Row(1, "aa", 41.0, 44.0), Row(1, "aa", 41.0, 45.0),
          Row(2, "bb", 10.0, 11.0), Row(2, "bb", 10.5, 11.0))
      )
    }
  }

  test("partitioned join: join on a subset of partition keys should trigger shuffle") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val items_partitions = Array(bucket(4, "id"), days("arrive_time"))
      createTable(items, items_schema, items_partitions,
        Distributions.clustered(items_partitions.toArray))
      sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.5, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

      val purchases_partitions = Array(bucket(4, "item_id"), days("time"))
      createTable(purchases, purchases_schema, purchases_partitions,
        Distributions.clustered(purchases_partitions.toArray))
      sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 44.0, cast('2020-01-15' as timestamp)), " +
        s"(1, 45.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 19.5, cast('2020-02-01' as timestamp))")

      val df = sql("SELECT id, name, i.price as purchase_price, p.price as sale_price " +
        s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
        "ON i.id = p.item_id ORDER BY id, purchase_price, sale_price")

      val shuffles = collect(df.queryExecution.executedPlan) {
        case s: ShuffleExchangeExec => s
      }
      // this includes a shuffle from the ORDER BY clause
      assert(shuffles.length == 3, "should not add shuffle for both sides of the join")
      checkAnswer(df,
        Seq(Row(1, "aa", 40.0, 42.0), Row(1, "aa", 40.0, 44.0), Row(1, "aa", 40.0, 45.0),
          Row(1, "aa", 41.0, 42.0), Row(1, "aa", 41.0, 44.0), Row(1, "aa", 41.0, 45.0),
          Row(2, "bb", 10.0, 11.0), Row(2, "bb", 10.5, 11.0), Row(3, "cc", 15.5, 19.5))
      )
    }
  }

  test("partitioned join: same partition keys but different order should trigger shuffle") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val items_partitions = Array(bucket(4, "id"), days("arrive_time"))
      createTable(items, items_schema, items_partitions,
        Distributions.clustered(items_partitions.toArray))
      sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.5, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

      val purchases_partitions = Array(days("time"), bucket(4, "item_id"))
      createTable(purchases, purchases_schema, purchases_partitions,
        Distributions.clustered(purchases_partitions.toArray))
      sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 44.0, cast('2020-01-15' as timestamp)), " +
        s"(1, 45.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 19.5, cast('2020-02-01' as timestamp))")

      val df = sql("SELECT id, name, i.price as purchase_price, p.price as sale_price " +
        s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
        "ON i.id = p.item_id AND i.arrive_time = p.time ORDER BY id, purchase_price, sale_price")

      val shuffles = collect(df.queryExecution.executedPlan) {
        case s: ShuffleExchangeExec => s
      }
      // this includes a shuffle from the ORDER BY clause
      assert(shuffles.length == 3, "should not add shuffle for both sides of the join")
      checkAnswer(df,
        Seq(Row(1, "aa", 40.0, 42.0), Row(1, "aa", 41.0, 44.0), Row(1, "aa", 41.0, 45.0),
          Row(2, "bb", 10.0, 11.0), Row(2, "bb", 10.5, 11.0), Row(3, "cc", 15.5, 19.5))
      )
    }
  }

  val test_schema: StructType = new StructType()
    .add("key", IntegerType)
    .add("data", StringType)
    .add("part_col1", LongType)
    .add("part_col2", TimestampType)

  test("partitioned join: 3-way join") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val test_partitions = Array(bucket(4, "part_col1"), days("part_col2"))
      createTable("table1", test_schema, test_partitions,
        Distributions.clustered(test_partitions.toArray))
      createTable("table2", test_schema, test_partitions,
        Distributions.clustered(test_partitions.toArray))
      createTable("table3", test_schema, test_partitions,
        Distributions.clustered(test_partitions.toArray))

      sql(s"INSERT INTO testcat.ns.table1 VALUES " +
        s"(11, 'aaa', 1, cast('2020-01-01' as date)), " +
        s"(12, 'aaa', 2, cast('2020-02-01' as date)), " +
        s"(13, 'aaa', 3, cast('2020-03-01' as date))")

        sql(s"INSERT INTO testcat.ns.table2 VALUES " +
        s"(21, 'bbb', 1, cast('2020-01-01' as date)), " +
        s"(22, 'bbb', 2, cast('2020-02-01' as date)), " +
        s"(23, 'bbb', 3, cast('2020-03-01' as date))")

        sql(s"INSERT INTO testcat.ns.table3 VALUES " +
        s"(31, 'ccc', 1, cast('2020-01-01' as date)), " +
        s"(32, 'ccc', 2, cast('2020-02-01' as date)), " +
        s"(33, 'ccc', 3, cast('2020-03-01' as date))")


      val df = sql("SELECT a.key, b.key, c.key " +
        s"FROM testcat.ns.table1 a JOIN testcat.ns.table2 b JOIN testcat.ns.table3 c " +
        "ON a.part_col1 = b.part_col1 AND a.part_col2 = b.part_col2 AND " +
        "b.part_col1 = c.part_col1 AND b.part_col2 = c.part_col2 ")

      val shuffles = collect(df.queryExecution.executedPlan) {
        case s: ShuffleExchangeExec => s
      }
      assert(shuffles.isEmpty)
    }
  }

  private def checkQueryPlan(
      df: DataFrame,
      distribution: physical.Distribution,
      ordering: Seq[SortOrder] = Seq.empty,
      partitioning: Partitioning): Unit = {
    // check distribution & ordering are correctly populated in logical plan
    val relation = df.queryExecution.optimizedPlan.collect {
      case r: DataSourceV2ScanRelation => r
    }.head

    val expectedDistribution = resolveDistribution(distribution, relation)
    val expectedOrdering = ordering.map(resolveAttrs(_, relation).asInstanceOf[SortOrder])

    assert(relation.distribution.isDefined)
    assert(relation.distribution.get == expectedDistribution)
    assert(relation.ordering == expectedOrdering)

    // check distribution, ordering and output partitioning are correctly populated in physical plan
    val scan = collect(df.queryExecution.executedPlan) {
      case s: BatchScanExec => s
    }.head

    val expectedPartitioning = resolvePartitioning(partitioning, scan)

    assert(scan.distribution.isDefined)
    assert(scan.distribution.get == expectedDistribution)
    assert(scan.ordering == expectedOrdering)
    assert(scan.outputPartitioning == expectedPartitioning)
  }

  private def createTable(
      table: String,
      schema: StructType,
      partitions: Array[Transform],
      distribution: V2Distribution = Distributions.unspecified(),
      ordering: Array[expressions.SortOrder] = Array.empty): Unit = {
    catalog.createTable(Identifier.of(Array("ns"), table), schema, partitions, emptyProps,
      distribution, ordering)
  }
}

// A test table and friends to complement test cases that `InMemoryTable` won't cover. This also
// test the non-catalog path.

class PartitionedDataSourceV2 extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    PartitionedDataSourceV2.schema
  }

  override def getTable(
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String]): Table = {
    new PartitionedBatchTable
  }
}

class PartitionedBatchTable extends Table with SupportsRead {
  override def name(): String = this.getClass.getCanonicalName
  override def schema(): StructType = PartitionedDataSourceV2.schema
  override def capabilities(): util.Set[TableCapability] = Set(BATCH_READ).asJava
  override def partitioning(): Array[Transform] = PartitionedDataSourceV2.partitioning
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new PartitionedScanBuilder
}

class PartitionedScanBuilder extends ScanBuilder with Scan with SupportsReportPartitioning {
  override def build(): Scan = this
  override def readSchema(): StructType = PartitionedDataSourceV2.schema
  override def toBatch: Batch = new PartitionedBatch

  override def outputPartitioning(): V2Partitioning = new V2Partitioning {
    override def distribution(): V2Distribution = Distributions.clustered(
      PartitionedDataSourceV2.partitioning.map(_.asInstanceOf[V2Expression]))
    override def ordering(): Array[V2SortOrder] = Array.empty
  }
}

class PartitionedBatch extends Batch {
  def utf8(str: String): UTF8String = UTF8String.fromString(str)

  override def planInputPartitions(): Array[InputPartition] = {
    // not all partitions implement `HasPartitionKey`
    Array(
      new TestInputPartitionWithPartitionKey(
        InternalRow(utf8("part2"), 50),
        Array((100, utf8("aaa"), utf8("part2"), createTimestamp("2020-01-01 12:00:00")),
          (200, utf8("bbb"), utf8("part2"), createTimestamp("2020-03-20 05:30:00")),
          (300, utf8("ccc"), utf8("part2"), createTimestamp("2020-10-10 09:00:00")))),
      TestInputPartition(
        Array((100, utf8("aaa"), utf8("part1"), createTimestamp("2021-01-01 00:00:00")),
          (200, utf8("bbb"), utf8("part1"), createTimestamp("2021-06-01 08:00:00")),
          (300, utf8("ccc"), utf8("part1"), createTimestamp("2021-12-01 12:00:00")))),
      new TestInputPartitionWithPartitionKey(
        InternalRow(utf8("part1"), 50),
        Array((100, utf8("aaa"), utf8("part1"), createTimestamp("2020-01-01 00:00:00")),
          (200, utf8("bbb"), utf8("part1"), createTimestamp("2020-10-01 12:00:30")),
          (300, utf8("ccc"), utf8("part1"), createTimestamp("2020-12-31 12:59:59"))))
    )
  }

  override def createReaderFactory(): PartitionReaderFactory = new PartitionedReaderFactory

  private def createTimestamp(s: String): Long = {
    DateTimeUtils.fromJavaTimestamp(java.sql.Timestamp.valueOf(s))
  }
}

class TestInputPartitionWithPartitionKey(
  partValue: InternalRow,
  values: Array[(Long, UTF8String, UTF8String, Long)])
  extends TestInputPartition(values) with HasPartitionKey {
  override def partitionKey(): InternalRow = partValue
}

case class TestInputPartition(
  values: Array[(Long, UTF8String, UTF8String, Long)]) extends InputPartition

class PartitionedReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val part = partition.asInstanceOf[TestInputPartition]
    new PartitionReader[InternalRow] {
      private var current = -1
      override def next(): Boolean = {
        current += 1
        current < part.values.length
      }
      override def get(): InternalRow = {
        val v = part.values(current)
        InternalRow(v._1, v._2, v._3, v._4)
      }
      override def close(): Unit = {}
    }
  }
}

object PartitionedDataSourceV2 {
  val schema: StructType = new StructType()
    .add("key", "bigint")
    .add("value", "string")
    .add("part_col1", "string")
    .add("part_col2", "timestamp")
  val partitioning: Array[Transform] =
    Array(
      bucket(32, "part_col1"),
      years("part_col2")
    )
}

