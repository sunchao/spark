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

package org.apache.spark.sql.connector.read.partitioning;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.SupportsReportPartitioning;

/**
 * An interface to represent the output data partitioning for a data source, which is returned by
 * {@link SupportsReportPartitioning#outputPartitioning()}. Note that this should work
 * like a snapshot. Once created, it should be deterministic and always report the same number of
 * partitions and the same "satisfy" result for a certain distribution.
 *
 * @since 3.0.0
 */
@Evolving
public interface Partitioning {
  /**
   * Returns the distribution guarantee that the data source provides.
   */
  Distribution distribution();

  /**
   * Returns the ordering guarantee that the data source provides.
   */
  SortOrder[] ordering();
}
