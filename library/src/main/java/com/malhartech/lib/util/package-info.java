/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 *
 */
/**
 * <b>{@link com.malhartech.lib.util}</b> is a library of utility classes<p>
 * <br>
 * <br> The classes are<br>
 * <b>{@link com.malhartech.lib.util.AbstractBaseFrequentKey}</b>: Occurrences of each key is counted in the input stream, and at the end of window the winning frequent key is emitted on output port "count"<p>
 * <b>{@link com.malhartech.lib.util.AbstractBaseFrequentKeyValue}</b>: Occurrences of all values for each key in a stream is counted and at the end of window the least frequent value is emitted on output port "count" per key<br>
 * <b>{@link com.malhartech.lib.util.AbstractBaseMatchOperator}</b>: An abstract class that sets up the basic operator functionality needed for match based operators<br>
 * <b>{@link com.malhartech.lib.util.AbstractBaseNOperator}</b>: Abstract class for basic topN operators; users need to provide processTuple, beginWindow, and endWindow to implement TopN operator<br>
 * <b>{@link com.malhartech.lib.util.AbstractBaseNNonUniqueOperator}</b>: Tuples are ordered by key, and bottom N of the ordered tuples per key are emitted at the end of window<br>
 * <b>{@link com.malhartech.lib.util.AbstractBaseNUniqueOperator}</b>: Abstract class for sorting NUnique key, val pairs, emit is done at end of window<<br>
 * <b>{@link com.malhartech.lib.util.AbstractBaseSortOperator}</b>: Takes a stream of key value pairs via input port "data"; The incoming tuple is merged into already existing sorted list. At the end of the window the entire sorted list is emitted on output port "sort"<br>
 * <b>{@link com.malhartech.lib.util.BaseKeyOperator}</b>: Base class for operators that allows cloneKey for mutable key objects<br>
 * <b>{@link com.malhartech.lib.util.BaseKeyValueOperator}</b>: extends {@link com.malhartech.lib.util.BaseKeyOperator} and enables cloneValue for mutable value objects<br>
 * <b>{@link com.malhartech.lib.util.BaseFilteredKeyValueOperator}</b>: extends {@link com.malhartech.lib.util.BaseKeyValueOperator} and enables filtering of keys<br>
 * <b>{@link com.malhartech.lib.util.BaseLineTokenizer}</b>: Base class for splitting lines into tokens and tokens into sub-tokens<br>
 * <b>{@link com.malhartech.lib.util.BaseMatchOperator}</b>:  * Base class that sets up the operator functionality needed for match based operators<br>
 * <b>{@link com.malhartech.lib.util.BaseNumberKeyValueOperator}</b>: Base class for operators that take in V extends Number. Provides basic methods for value conversion, and cloning of keys and values<br>
 * <b>{@link com.malhartech.lib.util.BaseNumberValueOperator}</b>: Base class for operators that take in K,V extends Number. Provides basic methods for value conversion and key cloning<br>
 * <b>{@link com.malhartech.lib.util.BaseUniqueKeyCounter}</b>: Count unique occurrences of keys within a window<<br>
 * <b>{@link com.malhartech.lib.util.BaseUniqueKeyValueCounter}</b>: Count unique occurrences of key,val pairs within a window<<br>
 * <b>{@link com.malhartech.lib.util.MutableDouble}</b>: A mutable double for basic operations. Makes things faster for incrementing etc. To be used for operations internal to operators<br>
 * <b>{@link com.malhartech.lib.util.MutableInteger}</b>: A mutable integer for basic operations. Makes things fasterfor incrementing etc. To be used for operations internal to operators<br>
 * <b>{@link com.malhartech.lib.util.MutableLong}</b>: A mutable long for basic operations. Makes things easy faster incrementing etc. To be used for operations internal to operators<br>
 * <b>{@link com.malhartech.lib.util.MutableShort}</b>: A mutable short for basic operations. Makes things faster for incrementing etc. To be used for operations internal to operators<br>
 * <b>{@link com.malhartech.lib.util.MutableFloat}</b>: A mutable float for basic operations. Makes things faster for incrementing etc. To be used for operations internal to operators<br>
 * <b>{@link com.malhartech.lib.util.OneKeyValPair}</b>: A single KeyValPair for basic data passing. It is a write once, and read often model<br>
 * <b>{@link com.malhartech.lib.util.ReversibleComparator}</b>: A comparator for ascending and descending lists<br>
 * <b>{@link com.malhartech.lib.util.TopNSort}</b>: Gives top N objects in ascending or descending order<br>
 * <b>{@link com.malhartech.lib.util.TopNUniqueSort}</b>: Gives top N objects in ascending or descending order and counts only unique objects<br>
 * <br>
 * <br>
 */
package com.malhartech.lib.util;
