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

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseNumberKeyValueOperator;
import com.datatorrent.lib.util.KeyValPair;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.lang.mutable.MutableLong;

/**
 *
 * Emits the average of values for each key at the end of window.
 * <p>
 * <br>User can block or only enable certain keys by setting filter-keys/inverse operator properties.
 * <br> Block Key : inverse=true
 * <br> Enable Key : inverse=false
 * This is an end window operator. This can not be partitioned. Partitioning
 * this will yield incorrect result.<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects KeyValPair&lt;K,V extends Number&gt;<br>
 * <b>averagePort(s)</b>: emits KeyValPair&lt;K,V extends Number&gt;</b><br>
 * <br>Output ports are optional.
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple (default : true).<br>
 * <b>filterBy</b>: List of keys to filter on.<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * @displayname: Average Key Value
 * @category: math
 * @tags: average, number, end window, key value
 * @since 0.3.3
 */
public class AverageKeyVal<K> extends BaseNumberKeyValueOperator<K, Number>
{
	// Aggregate sum of all values seen for a key.
	protected HashMap<K, MutableDouble> sums = new HashMap<K, MutableDouble>();
	
	// Count of number of values seen for key.
	protected HashMap<K, MutableLong> counts = new HashMap<K, MutableLong>();
	
	/**
	 * Input port that takes a key value pair.&nbsp; It adds the value for each key and counts the number of occurrences of each key to compute the average.
	 */
	@InputPortFieldAnnotation(name = "data")
	public final transient DefaultInputPort<KeyValPair<K, ? extends Number>> data = new DefaultInputPort<KeyValPair<K, ? extends Number>>()
	{
		/**
		 * Adds the values for each key, counts the number of occurrences of each
		 * key and computes the average.
		 */
		@Override
		public void process(KeyValPair<K, ? extends Number> tuple)
		{
			K key = tuple.getKey();
			if (!doprocessKey(key)) {
				return;
			}
			MutableDouble val = sums.get(key);
			if (val == null) {
				val = new MutableDouble(tuple.getValue().doubleValue());
			} else {
				val.add(tuple.getValue().doubleValue());
			}
			sums.put(cloneKey(key), val);

			MutableLong count = counts.get(key);
			if (count == null) {
				count = new MutableLong(0);
				counts.put(cloneKey(key), count);
			}
			count.increment();
		}
	};

	/**
	 * Double average output port. 
	 */
	@OutputPortFieldAnnotation(name = "doubleAverage", optional = true)
	public final transient DefaultOutputPort<KeyValPair<K, Double>> doubleAverage = new DefaultOutputPort<KeyValPair<K, Double>>();
	
	/**
	 * Integer average output port. 
	 */
	@OutputPortFieldAnnotation(name = "intAverage", optional = true)
	public final transient DefaultOutputPort<KeyValPair<K, Integer>> intAverage = new DefaultOutputPort<KeyValPair<K, Integer>>();
	
	/**
	 * Long average output port. 
	 */
	@OutputPortFieldAnnotation(name = "longAverage", optional = true)
	public final transient DefaultOutputPort<KeyValPair<K, Long>> longAverage = new DefaultOutputPort<KeyValPair<K, Long>>();

	/**
	 * Emits average for each key in end window. Data is computed during process
	 * on input port Clears the internal data before return.
	 */
	@Override
	public void endWindow()
	{
		for (Map.Entry<K, MutableDouble> e : sums.entrySet()) {
			K key = e.getKey();
			double d = e.getValue().doubleValue();
			if (doubleAverage.isConnected()) {
				doubleAverage.emit(new KeyValPair<K, Double>(key, d / counts.get(key).doubleValue()));
			}
			if (intAverage.isConnected()) {
				intAverage.emit(new KeyValPair<K, Integer>(key, (int) d));
			}
			if (longAverage.isConnected()) {
				longAverage.emit(new KeyValPair<K, Long>(key, (long) d));
			}
		}
		sums.clear();
		counts.clear();
	}
}
