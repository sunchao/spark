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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.SchemaPruningSuite
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.tags.ExtendedSQLTest

abstract class ParquetSchemaPruningSuite extends SchemaPruningSuite with AdaptiveSparkPlanHelper {
  override protected val dataSourceName: String = "parquet"
  override protected val vectorizedReaderEnabledKey: String =
    SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key
  override protected val vectorizedReaderNestedEnabledKey: String =
    SQLConf.ORC_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key

}

@ExtendedSQLTest
class ParquetV1SchemaPruningSuite extends ParquetSchemaPruningSuite {
  override protected def sparkConf: SparkConf =
    super
        .sparkConf
        .set(SQLConf.USE_V1_SOURCE_LIST, "parquet")

  test(s"Spark vectorized reader - without partition data column - test") {
    withSQLConf(vectorizedReaderEnabledKey -> "true") {
      withContacts {
        val query =
          sql("select id, name.last, name.middle, name.first, relatives[''].last, " +
              "friends[0].last, " +
              "pets, address from contacts where p=2")
        checkScan(query,
          "struct<id:int,name:struct<first:string,middle:string,last:string>,address:string," +
              "pets:int,friends:array<struct<last:string>>," +
              "relatives:map<string,struct<last:string>>>")
        checkAnswer(query.orderBy("id"),
          Row(2, "Jones", null, "Janet", null, null, null, "567 Maple Drive") ::
              Row(3, "Jones", null, "Jim", null, null, null, "6242 Ash Street") :: Nil)
      }
    }
  }

  test(s"Spark vectorized reader - single complex field array and its parent struct array") {
    withSQLConf(vectorizedReaderEnabledKey -> "true") {
      withContacts {
        val query = sql("select friends.middle, friends from contacts where p=1")
        checkScan(query,
          "struct<friends:array<struct<first:string,middle:string,last:string>>>")
        checkAnswer(query.orderBy("id"),
          Row(Array("Z."), Array(Row("Susan", "Z.", "Smith"))) ::
              Row(Array.empty[String], Array.empty[Row]) ::
              Nil)
      }
    }
  }
}

@ExtendedSQLTest
class ParquetV2SchemaPruningSuite extends ParquetSchemaPruningSuite {
  // TODO: enable Parquet V2 write path after file source V2 writers are workable.
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")

  override def checkScanSchemata(df: DataFrame, expectedSchemaCatalogStrings: String*): Unit = {
    val fileSourceScanSchemata =
      collect(df.queryExecution.executedPlan) {
        case scan: BatchScanExec => scan.scan.asInstanceOf[ParquetScan].readDataSchema
      }
    assert(fileSourceScanSchemata.size === expectedSchemaCatalogStrings.size,
      s"Found ${fileSourceScanSchemata.size} file sources in dataframe, " +
        s"but expected $expectedSchemaCatalogStrings")
    fileSourceScanSchemata.zip(expectedSchemaCatalogStrings).foreach {
      case (scanSchema, expectedScanSchemaCatalogString) =>
        val expectedScanSchema = CatalystSqlParser.parseDataType(expectedScanSchemaCatalogString)
        implicit val equality = schemaEquality
        assert(scanSchema === expectedScanSchema)
    }
  }
}
