/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 *
 */

/**
 * <b>com.malhartech.lib.algo</b> is a library of algorithmic modules for reuse<p>
 * <br>
 * <br>The modules are<br>
 * <b>{@link com.malhartech.lib.algo.AllOf}</b>: Takes in one stream via input port "data". Each tuple is tested for the compare function. The function is given by
 * "key", "value", and "compare". If all tuples passes a Boolean(true) is emitted, else a Boolean(false) is emitted on end of window on the output port "allof"<br>
 * <b>{@link com.malhartech.lib.algo.AnyOf}</b>: Takes in one stream via input port "data". Each tuple is tested for the compare function. The function is given by
 * "key", "value", and "compare". If any tuple passes a Boolean(true) is emitted, else a Boolean(false) is emitted on the output port "anyof".
 * The comparison is done by getting double value from the Number.<br>
 * <b>{@link com.malhartech.lib.algo.CountOf}</b>: Takes in one stream via input port "data". A count is done on how many tuples satisfy the compare function. The function is given by
 * "key", "value", and "compare". If a tuple passed the test count is incremented. On end of window count iss emitted on the output port "countof".
 * The comparison is done by getting double value from the Number.<br>
 * <b>{@link com.malhartech.lib.algo.Distinct}</b>: Takes a stream via input port "data" and emits distinct key,val pairs (i.e drops duplicates) on output port "distinct". Restarts at end of window boundary<br>
 * <b>{@link com.malhartech.lib.algo.FilterKeys}</b>: Takes a stream on input port "in_data", and outputs only keys specified by property "keys" on put output port out_data. If
 * property "inverse" is set to "true", then all keys except those specified by "keys" are emitted<br>
 * <b>{@link com.malhartech.lib.algo.FirstOf}</b>: Takes in one stream via input port "data". A compare function is imposed based on the property "key", "value", and "compare". If the tuple
 * passed the test, it is emitted on the output port "firstof". The comparison is done by getting double value from the Number. Both output ports are optional, but at least one has to be connected<br>
 * <b>{@link com.malhartech.lib.algo.GroupBy}</b>: Takes two streams via input port "in_data1" and "in_data2", and outputs GroupBy property "Key" on output port out_data<br>
 * <b>{@link com.malhartech.lib.algo.InvertIndexArray}</b>: Takes in a stream via input port "data". Inverts the index and sends out the tuple on output port "index"<br>
 * <b>{@link com.malhartech.lib.algo.InvertIndexMap}</b>: Takes in a stream via input port "data". Inverts the index and sends out the tuple on output port "index". Takes in specific queries on query port and outputs the data in the cache through console port on receiving the tuple and on each subsequent end_of_window tuple<br>
 * <b>{@link com.malhartech.lib.algo.ReverseIndex}</b>: Takes a stream via input port "data" and emits the reverse index on output port index<br>
 * <b>{@link com.malhartech.lib.algo.Sampler}</b>: Takes a stream via input port "data" and emits sample tuple on output port out_data<br>
 * <b>{@link com.malhartech.lib.algo.TupleQueue}</b>: * Takes in one stream via input port <b>data</b>. The data is key, value pair. It retains the last N values on that key. Output port gets the last N values. The node also provides a lookup via port <b>lookup</b><br>
 * <b>{@link com.malhartech.lib.algo.WindowedHolder}</b>: TBD<br>
 * <b>{@link com.malhartech.lib.algo.WindowTopCounter}</b>: TBD<br>
 * <br>
 */

package com.malhartech.lib.algo;
