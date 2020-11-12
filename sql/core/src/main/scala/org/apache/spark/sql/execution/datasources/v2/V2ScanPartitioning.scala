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

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Extract [[DataSourceV2ScanRelation]] from the input logical plan, convert any distribution and
 * ordering spec reported by data sources to their catalyst counterparts. Then, annotate the plan
 * with the result.
 */
object V2ScanPartitioning extends Rule[LogicalPlan] with SQLConfHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case d @ DataSourceV2ScanRelation(_, scan, _, _, _) =>
      val (distribution, ordering) = DistributionAndOrderingUtils.fromScan(d, scan)
      d.copy(distribution = distribution, ordering = ordering)
  }
}
