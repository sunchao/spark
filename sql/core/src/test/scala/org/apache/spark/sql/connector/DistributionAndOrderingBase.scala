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

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{catalyst, QueryTest}
import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.{physical, QueryPlan}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.test.SharedSparkSession

class DistributionAndOrderingBase
  extends QueryTest with SharedSparkSession with BeforeAndAfter with AdaptiveSparkPlanHelper {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.unsetConf("spark.sql.catalog.testcat")
  }

  protected val resolver: Resolver = conf.resolver

  protected def resolvePartitioning[T <: QueryPlan[T]](
      partitioning: Partitioning,
      plan: QueryPlan[T]): Partitioning = partitioning match {
    case HashPartitioning(exprs, numPartitions) =>
      HashPartitioning(exprs.map(resolveAttrs(_, plan)), numPartitions)
    case DataSourcePartitioning(clustering, partitionValues) =>
      DataSourcePartitioning(clustering.map(resolveAttrs(_, plan)), partitionValues)
    case PartitioningCollection(partitionings) =>
      PartitioningCollection(partitionings.map(resolvePartitioning(_, plan)))
    case RangePartitioning(ordering, numPartitions) =>
      RangePartitioning(ordering.map(resolveAttrs(_, plan).asInstanceOf[SortOrder]), numPartitions)
    case p @ SinglePartition =>
      p
    case p: UnknownPartitioning =>
      p
    case p =>
      fail(s"unexpected partitioning: $p")
  }

  protected def resolveDistribution[T <: QueryPlan[T]](
      distribution: physical.Distribution,
      plan: QueryPlan[T]): physical.Distribution = distribution match {
    case physical.ClusteredDistribution(clustering, numPartitions) =>
      physical.ClusteredDistribution(clustering.map(resolveAttrs(_, plan)), numPartitions)
    case physical.OrderedDistribution(ordering) =>
      physical.OrderedDistribution(ordering.map(resolveAttrs(_, plan).asInstanceOf[SortOrder]))
    case physical.UnspecifiedDistribution =>
      physical.UnspecifiedDistribution
    case d =>
      fail(s"unexpected distribution: $d")
  }

  protected def resolveAttrs[T <: QueryPlan[T]](
    expr: catalyst.expressions.Expression,
    plan: QueryPlan[T]): catalyst.expressions.Expression = {

    expr.transform {
      case UnresolvedAttribute(Seq(attrName)) =>
        plan.output.find(attr => resolver(attr.name, attrName)).get
      case UnresolvedAttribute(nameParts) =>
        val attrName = nameParts.mkString(".")
        fail(s"cannot resolve a nested attr: $attrName")
    }
  }

  protected def attr(name: String): UnresolvedAttribute = {
    UnresolvedAttribute(name)
  }

  protected def catalog: InMemoryTableCatalog = {
    val catalog = spark.sessionState.catalogManager.catalog("testcat")
    catalog.asTableCatalog.asInstanceOf[InMemoryTableCatalog]
  }
}
