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
package com.datatorrent.demos.uniquecountdemo;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.lib.util.KeyHashValPair;

import java.util.HashMap;
import java.util.Map;

/*
Compare results and print non-matching values to console.
 */
public class CountVerifier<K> implements Operator {
    HashMap<K, Integer> map1 = new HashMap<K, Integer>();
    HashMap<K, Integer> map2 = new HashMap<K, Integer>();

    public transient final DefaultInputPort<KeyHashValPair<K, Integer>> in1 =
            new DefaultInputPort<KeyHashValPair<K, Integer>>() {
                @Override
                public void process(KeyHashValPair<K, Integer> tuple) {
                    processTuple(tuple, map1);
                }
            };

    public transient final DefaultInputPort<KeyHashValPair<K, Integer>> in2 =
            new DefaultInputPort<KeyHashValPair<K, Integer>>() {
                @Override
                public void process(KeyHashValPair<K, Integer> tuple) {
                    processTuple(tuple, map2);
                }
            };

    void processTuple(KeyHashValPair<K, Integer> tuple, HashMap<K, Integer> map) {
        map.put(tuple.getKey(), tuple.getValue());
    }

    @Override
    public void beginWindow(long l) {

    }

    @Override
    public void endWindow() {
        boolean failure = false;
        for(Map.Entry<K, Integer> e : map1.entrySet()) {
            K key = e.getKey();
            int val = map2.get(key);
            if (val != e.getValue()) {
                System.out.println("Verification failed key " + key + " val " + e.getValue() + " actual " + "actual " + val);
                failure = true;
            }
        }
        if (!failure) {
            System.out.println("No error in this iteration.");
        }
    }

    @Override
    public void setup(Context.OperatorContext operatorContext) {

    }

    @Override
    public void teardown() {

    }
}
