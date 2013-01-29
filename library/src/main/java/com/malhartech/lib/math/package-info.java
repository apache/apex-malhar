/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 *
 */

/**
 * <b>{@link com.malhartech.lib.math}</b> is a library of arithmetic operators for reuse.<p>
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

package com.malhartech.lib.math;
