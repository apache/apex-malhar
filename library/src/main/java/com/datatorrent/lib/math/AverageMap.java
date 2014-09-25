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

import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.lang.mutable.MutableLong;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseNumberKeyValueOperator;

/**
 *
 * Emits the average value for each key at the end of window.
 * <p>
 * This is an end window operator. This can not be partitioned. Partitioning
 * this will yield incorrect result.<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects HashMap&lt;K,V extends Number&gt;<br>
 * <b>average</b>: emits HashMap&lt;K,V extends Number&gt;</b><br>
 * <br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <br>
 * @displayName Average Map
 * @category Math
 * @tags average, numeric, end window, key value, aggregate
 * @since 0.3.3
 */
public class AverageMap<K, V extends Number> extends
		BaseNumberKeyValueOperator<K, V>
{

	// Aggregate values in hash map.
	protected HashMap<K, MutableDouble> sums = new HashMap<K, MutableDouble>();
	protected HashMap<K, MutableLong> counts = new HashMap<K, MutableLong>();
	
	/**
	 * Input data port that takes a map of &lt;key,value&gt;.
	 */
	@InputPortFieldAnnotation(name = "data")
	public final transient DefaultInputPort<Map<K, V>> data = new DefaultInputPort<Map<K, V>>()
	{
		/**
		 * For each tuple (a HashMap of key,val pairs) adds the values for each key,
		 * counts the number of occurrences of each key and computes the average.
		 */
		@Override
		public void process(Map<K, V> tuple)
		{
			for (Map.Entry<K, V> e : tuple.entrySet()) {
				K key = e.getKey();
				if (!doprocessKey(key)) {
					continue;
				}

				MutableDouble val = sums.get(key);
				if (val == null) {
					val = new MutableDouble(e.getValue().doubleValue());
				} else {
					val.add(e.getValue().doubleValue());
				}
				sums.put(cloneKey(key), val);

				MutableLong count = counts.get(key);
				if (count == null) {
					count = new MutableLong(0);
					counts.put(cloneKey(key), count);
				}
				count.increment();
			}
		}
	};
	
	/**
	 *  Average output port that emits a hashmap.
	 */
	public final transient DefaultOutputPort<HashMap<K, V>> average = new DefaultOutputPort<HashMap<K, V>>()
	{
	};

	/**
	 * Emits average for each key in end window. Data is precomputed during
	 * process on input port Clears the internal data before return.
	 */
	@Override
	public void endWindow()
	{
		HashMap<K, V> atuples = new HashMap<K, V>();
		for (Map.Entry<K, MutableDouble> e : sums.entrySet()) {
			K key = e.getKey();
			atuples.put(key, getValue(e.getValue().doubleValue()
					/ counts.get(key).doubleValue()));
		}

		if (!atuples.isEmpty()) {
			average.emit(atuples);
		}
		sums.clear();
		counts.clear();
	}
}
