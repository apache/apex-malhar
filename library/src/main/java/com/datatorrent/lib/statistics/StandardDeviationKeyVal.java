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
package com.datatorrent.lib.statistics;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseNumberKeyValueOperator;
import com.datatorrent.lib.util.KeyValPair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This operator computes variance and standard deviation over incoming data for each given key-value pair. <br>
 * <br>
 * <b>Input Port(s) : </b><br>
 * <b>data : </b> expects KeyValPair&lt;K,V extends Number&gt;<br>
 * <br>
 * <b>Output Port(s) : </b> <br>
 * <b>variance : </b> emits variance value as KeyValPair&lt;K,V extends Number&gt;, one entry per key <br>
 * <b>standardDeviatin : </b>emits standard deviation value as KeyValPair&lt;K,V extends Number&gt;, one entry per key <br>
 * <br>
 * <b>StateFull : Yes</b>, value are aggregated over application window. <br>
 * <br>
 *
 * @since 0.4
 */
public class StandardDeviationKeyVal<K, V extends Number> extends BaseNumberKeyValueOperator<K, V> {

    public Map<K, List<V>> valuesMap = new HashMap<K, List<V>>();

    /**
     * Input data port.
     */
    @InputPortFieldAnnotation(name = "data")
    public final transient DefaultInputPort<KeyValPair<K, V>> data = new DefaultInputPort<KeyValPair<K, V>>() {
        /**
         * Computes sum and count with each tuple
         */
        @Override
        public void process(KeyValPair<K, V> tuple) {
            List<V> list = valuesMap.get(tuple.getKey());
            if (list == null) {
                list = new ArrayList<V>();
                valuesMap.put(tuple.getKey(), list);
            }
            list.add(tuple.getValue());
        }
    };

    /**
     * Variance output port
     */
    @OutputPortFieldAnnotation(name = "variance", optional = true)
    public final transient DefaultOutputPort<KeyValPair<K, Double>> variance =
            new DefaultOutputPort<KeyValPair<K, Double>>();

    /**
     * Standard deviation output port
     */
    @OutputPortFieldAnnotation(name = "standardDeviation")
    public final transient DefaultOutputPort<KeyValPair<K, Double>> standardDeviation =
            new DefaultOutputPort<KeyValPair<K, Double>>();

    /**
     * End window.
     */
    @Override
    public void endWindow() {
        // no values.
        if (valuesMap.size() == 0) return;

        // iterate over map and emit variance and standard-deviation
        for (Map.Entry<K, List<V>> entry: valuesMap.entrySet()) {
            this.emitResults(entry.getKey(), entry.getValue());
        }
        valuesMap.clear();
    }

    public void emitResults(K key, List<V> values) {

        // get mean first.
        double mean = 0.0;
        for (V value : values) {
            mean += value.doubleValue();
        }
        mean = mean / values.size();

        // get variance
        double outVal = 0.0;
        for (V value : values) {
            outVal += (value.doubleValue() - mean) * (value.doubleValue() - mean);
        }
        outVal = outVal / values.size();
        if (variance.isConnected()) {
            variance.emit(new KeyValPair<K, Double>(key, new Double(outVal)));
        }
        // get standard deviation
        standardDeviation.emit(new KeyValPair<K, Double>(key, new Double(Math.sqrt(outVal))));
    }
}
