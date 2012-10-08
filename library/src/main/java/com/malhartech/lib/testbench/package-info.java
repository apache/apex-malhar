/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 *
 */

/**
 * <b>com.malhartech.lib.testbench</b> is a library of test bench modules for reuse<p>
 * <br>
 * <br>The modules are<br>
 * <b>{@link com.malhartech.lib.testbench.FilterClassifier}</b>: Takes a in_stream <b>in_data</b> and filters the tuples. Only sends out tuples as per filter numbers provided on output port <b>out_data</b><br>
 * <b>{@link com.malhartech.lib.testbench.LoadClassifier}</b>: For every tuple appends a classifier key to the incoming key. If values are specified adds them to the incoming values<br>
 * <b>{@link com.malhartech.lib.testbench.LoadGenerator}</b>: Generates tuples for a given list of keys. Adds values to the tuple if they are specified. Allows limited randomization of probability<br>
 * <b>{@link com.malhartech.lib.testbench.LoadIncrementor}</b>: Takes in a seed stream on port <b>seed</b> and then on increments this data based on increments on port <b>increment</b>. Data is immediately emitted on output port <b>data</b><br>
 * <b>{@link com.malhartech.lib.testbench.LoadRandomGenerator}</b>: Generates synthetic load. Creates tuples using random numbers and keeps emitting them on the output port "data"<br>
 * <b>{@link com.malhartech.lib.testbench.LoadSeedGenerator}</b>: Generates one time seed load based on range provided for the keys, and adds new classification to incoming keys. The new tuple is emitted on the output port <b>data</b><br>
 * <b>{@link com.malhartech.lib.testbench.SeedClassifier}</b>: Generates seeds and merges data as it comes in from input ports (<b>in_data1</b>, and <b>in_data2</b>. The new tuple is emitted on the output port <b>out_data</b>
 * <b>{@link com.malhartech.lib.testbench.StreamMerger}</b>: Merges two streams with identical schema and emits the tuples to the output port in order<br>
 * <b>{@link com.malhartech.lib.testbench.StreamMerger5}</b>: Merges five streams with identical schema and emits tuples on to the output port in order<br>
 * <b>{@link com.malhartech.lib.testbench.StreamMerger10}</b>: Merges ten streams with identical schema and emits tuples on to the output port in order<br>
 * <b>{@link com.malhartech.lib.testbench.ThrougputCounter}</b>: Takes a in stream <b>data</b> as a HashMap<String, Integer> and add all integer values. On end of window this total and average is emitted on output port <b>count</b><br>
 * <br>
 * <br>
 */

package com.malhartech.lib.testbench;
