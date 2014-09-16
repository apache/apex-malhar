/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
 * limitations under the License.
 */
package com.datatorrent.lib.math;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableInt;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseKeyValueOperator;
import com.datatorrent.lib.util.UnifierHashMapInteger;

/**
 * Operator aggregates &lt;key,occurrence&gt; count in map from &lt;key,value&gt; map input port.
 * <p>
 * Emits the count of occurrences of each key at the end of window. <br>
 * <br>
 * StateFull : Yes, each key occurrence are counted till end windows is seen. <br>
 * Partitions : Yes, hash sum unifier on output port. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,V extends Number&gt;<br>
 * <b>count</b>: emits HashMap&lt;K,Integer&gt;<br>
 * <br>
 * <b>Properties</b>: <br>
 *  counts : Key occurrence aggregate map.
 * <br>
 * @displayname: Count Map
 * @category: lib.math
 * @tags: count, key value, aggregate, Map
 * @since 0.3.3
 */
public class CountMap<K, V> extends BaseKeyValueOperator<K, V>
{
	/**
	 * Aggregate key occurrence count.
	 */
	protected HashMap<K, MutableInt> counts = new HashMap<K, MutableInt>();
	
	/**
	 * Input port to receive data.
	 */
	@InputPortFieldAnnotation(name = "data")
	public final transient DefaultInputPort<Map<K, V>> data = new DefaultInputPort<Map<K, V>>()
	{
		/**
		 * For each tuple (a HashMap of keys,val pairs) Adds the values for each
		 * key.
		 */
		@Override
		public void process(Map<K, V> tuple)
		{
			for (Map.Entry<K, V> e : tuple.entrySet()) {
				K key = e.getKey();
				MutableInt val = counts.get(key);
				if (val == null) {
					val = new MutableInt();
					counts.put(key, val);
				}
				val.increment();
			}
		}
	};

	@OutputPortFieldAnnotation(name = "count")
	public final transient DefaultOutputPort<HashMap<K, Integer>> count = new DefaultOutputPort<HashMap<K, Integer>>()
	{
		@Override
		public Unifier<HashMap<K, Integer>> getUnifier()
		{
			return new UnifierHashMapInteger<K>();
		}
	};

	/**
	 * Emits on all ports that are connected. Data is precomputed during process
	 * on input port endWindow just emits it for each key Clears the internal data
	 * before return
	 */
	@Override
	public void endWindow()
	{
		HashMap<K, Integer> tuples = new HashMap<K, Integer>();
		for (Map.Entry<K, MutableInt> e : counts.entrySet()) {
			tuples.put(e.getKey(), e.getValue().intValue());
		}
		if (!tuples.isEmpty()) {
			count.emit(tuples);
		}
		clearCache();
	}

	public void clearCache()
	{
		counts.clear();
	}
}
