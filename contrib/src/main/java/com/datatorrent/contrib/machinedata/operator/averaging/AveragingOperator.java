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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>AveragingOperator class.</p>
 *
 * @since 0.3.5
 */
public class AveragingOperator<ValueKey> extends BaseOperator {

    private Map<TimeBucketKey, List<Map<ValueKey, AverageData>>> dataMap =
            new HashMap<TimeBucketKey, List<Map<ValueKey, AverageData>>>();

    public final transient DefaultOutputPort<KeyValPair<TimeBucketKey, Map<ValueKey, Double>>> outputPort =
            new DefaultOutputPort<KeyValPair<TimeBucketKey, Map<ValueKey, Double>>>();

    /**
     * Buffer all the tuples as is till end window gets called
     */
    public final transient DefaultInputPort<KeyValPair<TimeBucketKey, Map<ValueKey, AverageData>>> inputPort =
        new DefaultInputPort<KeyValPair<TimeBucketKey, Map<ValueKey, AverageData>>>() {
            @Override
            public void process(KeyValPair<TimeBucketKey, Map<ValueKey, AverageData>> tuple) {
                TimeBucketKey key = tuple.getKey();
                List<Map<ValueKey, AverageData>> list = dataMap.get(key);
                if (list == null) {
                    list = new ArrayList<Map<ValueKey, AverageData>>();
                    list.add(tuple.getValue());
                    dataMap.put(key, list);
                }
            }
        };

    @Override
    public void endWindow() {
        for (Map.Entry<TimeBucketKey, List<Map<ValueKey, AverageData>>> entry: dataMap.entrySet()) {
            TimeBucketKey key = entry.getKey();
            List<Map<ValueKey, AverageData>> list = entry.getValue();
            Map<ValueKey, AverageData> averageResultMap = new HashMap<ValueKey, AverageData>();
            for (Map<ValueKey, AverageData> map: list) {
                for (Map.Entry<ValueKey, AverageData> dataEntry: map.entrySet()) {
                    ValueKey valueKey = dataEntry.getKey();
                    AverageData average = averageResultMap.get(valueKey);
                    if (average == null) {
                        average = new AverageData(dataEntry.getValue().getSum(),
                                dataEntry.getValue().getCount());
                        averageResultMap.put(valueKey,average);
                    } else {
                        average.setSum(average.getSum() + dataEntry.getValue().getSum());
                        average.setCount(average.getCount() + dataEntry.getValue().getCount());
                    }
                }
            }
            Map<ValueKey, Double> averageResult = new HashMap<ValueKey, Double>();
            for (Map.Entry<ValueKey, AverageData> dataEntry: averageResultMap.entrySet()) {
                ValueKey valueKey = dataEntry.getKey();
                double average = dataEntry.getValue().getSum() / dataEntry.getValue().getCount();
                averageResult.put(valueKey, average);
            }
            outputPort.emit(new KeyValPair<TimeBucketKey, Map<ValueKey, Double>>(key, averageResult));
        }
    }
}
