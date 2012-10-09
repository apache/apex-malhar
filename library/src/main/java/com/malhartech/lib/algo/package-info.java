/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 *
 */

/**
 * <b>com.malhartech.lib.algo</b> is a library of algorithmic modules for reuse<p>
 * <br>
 * <br>The modules are<br>
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
