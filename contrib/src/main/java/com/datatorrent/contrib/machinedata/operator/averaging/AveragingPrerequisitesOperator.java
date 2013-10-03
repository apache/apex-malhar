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
package com.datatorrent.contrib.machinedata.operator.averaging;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.lib.util.TimeBucketKey;
import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.lang.mutable.MutableLong;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>AveragingPrerequisitesOperator class.</p>
 *
 * @since 0.3.5
 */
public class AveragingPrerequisitesOperator<T extends AveragingInfo, ValueKey, NumType> extends BaseOperator {

    // Aggregate sum of all values seen for a key.
    private HashMap<TimeBucketKey, Map<ValueKey, MutableDouble>> sums = new HashMap<TimeBucketKey, Map<ValueKey, MutableDouble>>();

    // Count of number of values seen for key.
    private HashMap<TimeBucketKey, MutableLong> counts = new HashMap<TimeBucketKey, MutableLong>();

    public final transient DefaultOutputPort<KeyValPair<TimeBucketKey, Map<ValueKey, AverageData>>> outputPort =
            new DefaultOutputPort<KeyValPair<TimeBucketKey, Map<ValueKey, AverageData>>>();

    public transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>() {

        @Override
        public void process(T tuple) {
            TimeBucketKey key = tuple.getAveragingKey();
            Map<ValueKey, NumType> dataMap = tuple.getDataMap();

            Map<ValueKey, MutableDouble> sumsMap = sums.get(key);
            if (sumsMap == null) {
                sumsMap = new HashMap<ValueKey, MutableDouble>();
                for (Map.Entry<ValueKey, NumType> entry: dataMap.entrySet()) {
                    ValueKey valueKey = entry.getKey();
                    Number number = (Number)entry.getValue();
                    sumsMap.put(valueKey, new MutableDouble(number));
                }
                sums.put(key, sumsMap);
            } else {
                for (Map.Entry<ValueKey, NumType> entry: dataMap.entrySet()) {
                    ValueKey valueKey = entry.getKey();
                    Number number = (Number)entry.getValue();
                    sumsMap.get(valueKey).add(number);
                }
            }

            MutableLong count = counts.get(key);
            if (count == null) {
                count = new MutableLong(1);
                counts.put(key, count);
            } else {
                count.increment();
            }
        }
    };

    @Override
    public void endWindow() {

        for (Map.Entry<TimeBucketKey, Map<ValueKey, MutableDouble>> entry : sums.entrySet()) {

            Map<ValueKey, MutableDouble> sumMap = sums.get(entry.getKey());
            long count = counts.get(entry.getKey()).longValue();

            Map<ValueKey, AverageData> avg = new HashMap<ValueKey, AverageData>();
            for (Map.Entry<ValueKey, MutableDouble> sumMapEntry: sumMap.entrySet()) {
                ValueKey key = sumMapEntry.getKey();
                avg.put(key, new AverageData(sumMap.get(key).doubleValue(), count));
            }

            outputPort.emit(new KeyValPair<TimeBucketKey, Map<ValueKey, AverageData>>(entry.getKey(), avg));
        }
        sums.clear();
        counts.clear();
    }

}
