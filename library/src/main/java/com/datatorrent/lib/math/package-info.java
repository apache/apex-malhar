/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

/**
 * <b>com.datatorrent.lib.math</b> is a library of arithmetic operators for reuse.<p>
 * <br>
 * Most of the arithmetic operators come in three types based on their schema<br>
 * The operators whose names ends with "Map" (eg SumMap, MaxMap, MinMap) take in Map on input ports and emit HashMap. These operators use
 * round robin partitioning and would merge as per their functionality<br>
 * The operators whose names ends with "KeyVal" (eg SumKeyVal, MaxKeyVal, MinKeyVal) take in KeyValPair and emit KeyValPair. These operators use
 * sticky key partitioning and would merge using default pass through merge operator<br>
 * The operators whose names are just their function name (eg Sum, Min, Max) operate on same objects and emit a final result. These operators have no keys.
 * They partition in roundrobin and would merge as per their functionality<br>
 * Average, Quotient operators are non-partitionable<br>
 * <br>
 * <br>
 */

package com.datatorrent.lib.math;
