#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

ICEBERG_VERSION_PROPS=$1

ICEBERG_HIVE=`sed -n 's/org\.apache\.hive:\* = \(.*\)/\1/p' $ICEBERG_VERSION_PROPS`
SPARK_HIVE=`sed -n 's/<hive\.version>\(.*\)<\/hive\.version>/\1/p' pom.xml | xargs`

if [ "$ICEBERG_HIVE" = "$SPARK_HIVE" ]; then
  echo "Iceberg Hive version $ICEBERG_HIVE equal to Spark's $SPARK_HIVE"
  exit 0
else
  echo "Iceberg Hive version $ICEBERG_HIVE not equal to Spark's $SPARK_HIVE!"
  exit 1
fi
