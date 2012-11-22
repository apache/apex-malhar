/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 *
 */

/**
 * <b>{@link com.malhartech.lib.algo}</b> is a library of algorithmic operators<p>
 * <br>
 * <br>The modules are<br>
 * <b>{@link com.malhartech.lib.algo.AllAfterMatch}</b>:  * Takes in one stream via input port "data". A compare function is imposed based on the property "key", "value", and "compare". All tuples
 * are emitted (inclusive) once a match is made. The comparison is done by getting double value from the Number.<br>
 * <b>{@link com.malhartech.lib.algo.AllAfterMatchStringValue}</b>: Takes in one stream via input port "data". A compare function is imposed based on the property "key", "value", and "compare". All tuples
 * are emitted (inclusive) once a match is made. The comparison is done by getting double value from the Number.<br>
 * <b>{@link com.malhartech.lib.algo.BottomN}</b>: Takes a stream of key value pairs via input port "data", and they are ordered by key. Bottom N of the ordered tuples per key are emitted on port "bottom" at the end of window<br>
 * <b>{@link com.malhartech.lib.algo.BottomNUnique}</b>: Takes a stream of key value pairs via input port "data", and they are ordered by key. Bottom N of the ordered unique tuples per key are emitted on port "top" at the end of window<br>
 * <b>{@link com.malhartech.lib.algo.CompareExceptCount}</b>: Takes in one stream via input port "data". A count is done on how many tuples satisfy the compare function. The function is given by
 * "key", "value", and "compare". If a tuple passed the test count is incremented. On end of window count is emitted on the output port "count". The comparison is done by getting double value from the Number.<br>
 * <b>{@link com.malhartech.lib.algo.CompareExceptCountString}</b>: Takes in one stream via input port "data". A count is done on how many tuples satisfy the compare function. The function is given by
 * "key", "value", and "compare". If a tuple passed the test count is incremented. On end of window count is emitted on the output port "count". The comparison is done by getting double value from the Number.<br>
 * <b>{@link com.malhartech.lib.algo.Distinct}</b>: Takes a stream via input port "data" and emits distinct key,val pairs (i.e drops duplicates) on output port "distinct". Restarts at end of window boundary<br>
 * <b>{@link com.malhartech.lib.algo.FilterKeys}</b>: Filters the incoming stream based of keys specified by property "keys". If property "inverse" is set to "true", then all keys except those specified by "keys" are emitted<br>
 * <b>{@link com.malhartech.lib.algo.FilterKeyVals}</b>: Filters the incoming stream based of specified key,val pairs, and emits those that match the filter. If
 * property "inverse" is set to "true", then all key,val pairs except those specified by in keyvals parameter are emitted<br:
 * <b>{@link com.malhartech.lib.algo.FilterValues}</b>: Takes a stream on input port "data", and outputs only values as specified by the user on put output port "filter". If
 * property "inverse" is set to "true", then all keys except those specified by "keys" are emitted. The values are expected to be immutable<br>
 * <b>{@link com.malhartech.lib.algo.FirstMatch}</b>: Takes in one stream via input port "data". A compare function is imposed based on the property "key", "value", and "compare". If the tuple
 * passed the test, it is emitted on the output port "first". The comparison is done by getting double value from the Number. Both output ports are optional, but at least one has to be connected<br>
 * <b>{@link com.malhartech.lib.algo.FirstMatchString}</b>: Takes in one stream via input port "data". A compare function is imposed based on the property "key", "value", and "compare". If the tuple
 * passed the test, it is emitted on the output port "first" as the first match. The comparison is done by getting double value from the Number.<br>
 * <b>{@link com.malhartech.lib.algo.FirstN}</b>: Takes in one stream via input port "data". Takes the first N tuples of a particular key and emits them as they come in on output port "first"<br>
 * <b>{@link com.malhartech.lib.algo.FirstTillMatch}</b>: Takes in one stream via input port "data". A compare function is imposed based on the property "key", "value", and "compare". All tuples
 * are emitted till a tuple matches this test. Then on no tuple is emitted in that window. The comparison is done by getting double value from the Number.<br>
 * <b>{@link com.malhartech.lib.algo.FirstTillMatchString}</b>: Takes in one stream via input port "data". A compare function is imposed based on the property "key", "value", and "compare". All tuples
 * are emitted till a tuple matches this test. Then on no tuple is emitted in that window. The comparison is done by getting double value from the Number.<br>
 * <b>{@link com.malhartech.lib.algo.GroupBy}</b>: Takes two streams via input port "in_data1" and "in_data2", and outputs GroupBy property "Key" on output port out_data<br>
 * <b>{@link com.malhartech.lib.algo.InsertSort}</b>: <b>Not done yet</b> Takes a stream of key value pairs via input port "data". The incoming is merged into already existing sorted list.
 * At the end of the window the entire sorted list is emitted on output port "sort"<br>
 * <b>{@link com.malhartech.lib.algo.InvertIndex}</b>: Takes in a stream via input port "data". Inverts the index and sends out the tuple on output port "index" at the end of the window<br>
 * <b>{@link com.malhartech.lib.algo.InvertIndexArray}</b>: Takes in a stream via input port "data". Inverts the index and sends out the tuple on output port "index"<br>
 * <b>{@link com.malhartech.lib.algo.InvertIndexUniqueMap}</b>: Takes in a stream via input port "data". Inverts the index and sends out the tuple on output port "index". Takes in specific queries on query port and outputs the data in the cache through console port on receiving the tuple and on each subsequent end_of_window tuple<br>
 * <b>{@link com.malhartech.lib.algo.LastMatch}</b>: Takes in one stream via input port "data". A compare function is imposed based on the property "key", "value", and "compare". Every tuple
 * is checked and the last one that passes the condition is send during end of window on port "last". The comparison is done by getting double
 * value from the Number<br>
 * <b>{@link com.malhartech.lib.algo.LastMatch}</b>: Takes in one stream via input port "data". A compare function is imposed based on the property "key", "value", and "compare". Every tuple
 * is checked and the last one that passes the condition is send during end of window on port "last". The comparison is done by getting double
 * value from the Number<br>
 * <b>{@link com.malhartech.lib.algo.LeastFrequentKey}</b>: Occurrences of each tuple is counted and at the end of window any of the least frequent tuple is emitted on output port least and all least frequent tuples on output port list<p>
 * <b>{@link com.malhartech.lib.algo.LeastFrequentKeyInMap}</b>: Occurrences of each key is counted in a HashMap and at the end of window any of the least frequent key is emitted on output port least and all least frequent keys on output port list<p>
 * <b>{@link com.malhartech.lib.algo.LeastFrequentKeyValue}</b>: Takes in one stream via input port "data". Occurrences of all values for each key is counted and at the end of window the least frequent value is emitted
 * on output port "count" per key<br>
 * <b>{@link com.malhartech.lib.algo.Match}</b>: Takes in one stream via input port "data". A compare function is imposed based on the property "key", "value", and "compare". If the tuple
 * passed the test, it is emitted on the output port "match". The comparison is done by getting double value from the Number. Both output ports are optional, but at least one has to be connected<br>
 * <b>{@link com.malhartech.lib.algo.MatchAll}</b>: Takes in one stream via input port "data". Each tuple is tested for the compare function. The function is given by
 * "key", "value", and "compare". If all tuples passes a Boolean(true) is emitted, else a Boolean(false) is emitted on end of window on the output port "all"<br>
 * <b>{@link com.malhartech.lib.algo.MatchAllString}</b>: Takes in one stream via input port "data". Each tuple is tested for the compare function. The function is given by
 * "key", "value", and "compare". If all tuples passes a Boolean(true) is emitted, else a Boolean(false) is emitted on end of window on the output port "all". The comparison is done by getting double value from the Number.<br>
 * <b>{@link com.malhartech.lib.algo.MatchAny}</b>: Takes in one stream via input port "data". Each tuple is tested for the compare function. The function is given by
 * "key", "value", and "compare". If any tuple passes a Boolean(true) is emitted, else a Boolean(false) is emitted on the output port "any". The comparison is done by getting double value from the Number.<br>
 * <b>{@link com.malhartech.lib.algo.MatchAnyString}</b>: Takes in one stream via input port "data". Each tuple is tested for the compare function. The function is given by
 * "key", "value", and "compare". If any tuple passes a Boolean(true) is emitted, else a Boolean(false) is emitted on the output port "any". The comparison is done by getting double value from the Number.<br>
 * <b>{@link com.malhartech.lib.algo.MatchString}</b>: Takes in one stream via input port "data". A compare function is imposed based on the property "key", "value", and "compare". If the tuple
 * passed the test, it is emitted on the output port "match". The comparison is done by getting double value from the Number. Both output ports are optional, but at least one has to be connected<br>
 * <b>{@link com.malhartech.lib.algo.MergeSort}</b>: <b>Not done yet</b> Takes a stream of key value pairs via input port "data". The incoming data is expected to be sorted list. The tuple are then merged into already existing sorted list.
 * At the end of the window the entire sorted list is emitted on output port "sort"<br>
 * <b>{@link com.malhartech.lib.algo.MostFrequentKey}</b>: Takes in one stream via input port "data". Occurrences of each key is counted and at the end of window the most frequent key is emitted on output port "count"<br>
 *  This module is an end of window module<br>
 * <b>{@link com.malhartech.lib.algo.MostFrequentKeyInMap}</b>: Takes in one stream via input port "data". Occurrences of each key is counted and at the end of window the most frequent key is emitted on output port "count"<br>
 * <b>{@link com.malhartech.lib.algo.MostFrequentKeyValue}</b>: Takes in one stream via input port "data". Occurrences of all values for each key is counted and at the end of window the most frequent value is emitted
 * on output port "count" per key<br>
 * <b>{@link com.malhartech.lib.algo.OrderByKey}</b>: Takes a stream of key value pairs via input port "data", and they are ordered by a given key. The ordered tuples are emitted on port "out_data" at the end of window<br>
 * <b>{@link com.malhartech.lib.algo.OrderByKeyDesc}</b>: Takes a stream of key value pairs via input port "data", and they are ordered by a given key. The descending ordered tuples are emitted on port "out_data" at the end of window<br>
 * <b>{@link com.malhartech.lib.algo.OrderByValue}</b>: Takes a stream of key value pairs via input port "data", and they are ordered by their value. The ordered tuples are emitted on port "out_data" at the end of window<br>
 * <b>{@link com.malhartech.lib.algo.OrderByValueDesc}</b>: Takes a stream of key value pairs via input port "data", and they are ordered by their value in a descending manner. The ordered tuples are emitted on port "out_data" at the end of window<br>
 * <b>{@link com.malhartech.lib.algo.TopN}</b>: Takes a stream of key value pairs via input port "data", and they are ordered by key. Top N of the ordered tuples per key are emitted on port "top" at the end of window<br>
 * <b>{@link com.malhartech.lib.algo.TopNUnique}</b>: Takes a stream of key value pairs via input port "data", and they are ordered by key. Top N of the ordered unique tuples per key are emitted on port "top" at the end of window<br>
 * <b>{@link com.malhartech.lib.algo.Sampler}</b>: Takes a stream via input port "data" and emits sample tuple on output port out_data<br>
 * <b>{@link com.malhartech.lib.algo.TupleQueue}</b>: Takes in one stream via input port <b>data</b>. The data is key, value pair. It retains the last N values on that key. Output port gets the last N values. The node also provides a lookup via port <b>lookup</b><br>
 * <b>{@link com.malhartech.lib.algo.UniqueCounter}</b>: Count unique occurrences of keys within a window and emits a single HashMap<br>
 * <b>{@link com.malhartech.lib.algo.UniqueCounterEach}</b>: Count unique occurrences of keys within a window and emits one HashMap(1) per unique key<br>
 * <b>{@link com.malhartech.lib.algo.UniqueKeyValCounter}</b>: Count unique occurrences of key,val pairs within a window and emits a single HashMap<br>
 * <b>{@link com.malhartech.lib.algo.UniqueKeyValCounterEach}</b>: Count unique occurrences of key,val pairs within a window, and emits a tuple per unique key,val pair<br>
 * <b>{@link com.malhartech.lib.algo.WindowedHolder}</b>: TBD<br>
 * <b>{@link com.malhartech.lib.algo.WindowedTopCounter}</b>: TBD<br>
 * <br>
 */

package com.malhartech.lib.algo;
