/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 * Library of arithmetic operators.
 * <br>
 * Most of the arithmetic operators come in three types based on their schema.
 * The operators whose names ends with "Map" (eg SumMap, MaxMap, MinMap) take in Map on input ports and emit HashMap. These operators use
 * round robin partitioning and would merge as per their functionality.
 * <br>
 * The operators whose names ends with "KeyVal" (eg SumKeyVal, MaxKeyVal, MinKeyVal) take in KeyValPair and emit KeyValPair. These operators use
 * sticky key partitioning and would merge using default pass through merge operator.
 * <br>
 * The operators whose names are just their function name (eg Sum, Min, Max) operate on same objects and emit a final result. These operators have no keys.
 * They partition in roundrobin and would merge as per their functionality.
 * <br>
 * Average, Quotient operators are non-partitionable.
 * <br>
 * <br>
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
package org.apache.apex.malhar.lib.math;
