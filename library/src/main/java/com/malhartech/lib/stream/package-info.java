/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 *
 */

/**
 * <b>{@link com.malhartech.lib.stream}</b> is a library operators for stream operations<p>
 * <br>
 * <br> The classes are<br>
 * <b>{@link com.malhartech.lib.stream.ArrayListToItem}</b>: Takes in an ArrayList and emits each item in the array. Used for breaking up a ArrayList into Objects<br>
 * <b>{@link com.malhartech.lib.stream.DevNullCounter}</b>: Counts and then drops the tuple. Used for logging. Increments a count and writes the net number (rate) to console. Mainly to be used to benchmark other modules<br>
 * <b>{@link com.malhartech.lib.stream.HashMapToKey}</b>: Takes a HashMap and emits its keys, keyvals, vals. Used for breaking up a HashMap ito objects (keys, vals, or key/val pairs)<br>
 * <b>{@link com.malhartech.lib.stream.StreamDuplicater}</b>: Takes one stream and emits exactly same tuple on both the output ports. Needed to allow separation of listeners into two streams<br>
 * <b>{@link com.malhartech.lib.stream.StreamMerger}</b>: Merges two streams with identical schema and emits the tuples to the output port in order<br>
 * <b>{@link com.malhartech.lib.stream.StreamMerger10}</b>: Merges up to ten streams with identical schema and emits the tuples to the output port in order<br>
 * <b>{@link com.malhartech.lib.stream.StreamMerger5}</b>: Merges up to five streams with identical schema and emits the tuples to the output port in order<br>
 * <br>
 * <br>
 */

package com.malhartech.lib.stream;
