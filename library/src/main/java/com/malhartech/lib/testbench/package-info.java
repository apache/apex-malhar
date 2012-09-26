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
 * <b>{@link com.malhartech.lib.testbench.LoadRandomGenerator}</b>: Generates synthetic load. Creates tuples using random numbers and keeps emitting them on the output port "data"<br>
 * <b>{@link com.malhartech.lib.testbench.LoadSeedGenerator}</b>: Generates one time seed load based on range provided for the keys, and adds new classification to incoming keys. The new tuple is emitted on the output port <b>data</b><br>
 * <b>{@link com.malhartech.lib.testbench.SeedClassifier}</b>: Generates seeds and merges data as it comes in from input ports (<b>in_data1</b>, and <b>in_data2</b>. The new tuple is emitted on the output port <b>out_data</b>
 * <br>
 *
 */

package com.malhartech.lib.testbench;
