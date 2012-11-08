/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 *
 */

/**
 * <b>com.malhartech.lib.testbench</b> is a library of test bench modules for reuse<p>
 * <br>
 * <br>The modules are<br>
 * <b>{@link com.malhartech.lib.testbench.EventClassifier}</b>: Takes a in stream event and adds to incoming keys to create a new tuple that is emitted on output port data. The aim is to create a load with pair of keys<br>
 * <b>{@link com.malhartech.lib.testbench.EventGenerator}</b>: Generates synthetic load. Creates tuples and keeps emitting them on the output port data<br>
 * <b>{@link com.malhartech.lib.testbench.EventIncrementor}</b>: Takes in a seed stream on port seed and then on increments this data based on increments on port increment. Data is immediately emitted on output port data. Emits number of tuples on port count<br>
 * <b>{@link com.malhartech.lib.testbench.FilterClassifier}</b>: Takes in a stream data and filters the tuples. Only sends out tuples as per filter numbers provided on output port filter. The aim is to create another stream representing a subsection of incoming load<br>
 * <b>{@link com.malhartech.lib.testbench.FilteredEventClassifier}</b>: Takes in a stream data and filters the tuples. Only sends out tuples as per filter numbers provided on output port filter. The aim is to create another stream representing a subsection of incoming load<br>
 * <b>{@link com.malhartech.lib.testbench.RandomEventGenerator}</b>: Generates synthetic load. Creates tuples using random numbers and keeps emitting them on the output port string_data and integer_data<br> * <b>{@link com.malhartech.lib.testbench.LoadSeedGenerator}</b>: Generates one time seed load based on range provided for the keys, and adds new classification to incoming keys. The new tuple is emitted on the output port <b>data</b><br>
 * <b>{@link com.malhartech.lib.testbench.SeedEventClassifier}</b>:  * Generates seeds and merges data as it comes in from input ports (data1, and data2). The new tuple is emitted on the output ports string_data and hash_data<br>
 * <b>{@link com.malhartech.lib.testbench.SeedEventGenerator}</b>:  * Generates one time seed load based on range provided for the keys, and adds new classification to incoming keys. The new tuple is emitted on the output port keyvalpair_list, val_list, string_data, val_data<br>
 * <b>{@link com.malhartech.lib.testbench.ThrougputCounter}</b>: Takes a in stream data as a HashMap<String, Integer> and add all integer values. On end of window this total and average is emitted on output port count<br>
 * <br>
 * <br>
 */

package com.malhartech.lib.testbench;
