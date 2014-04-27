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
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.lib.util.KeyValPair;
import java.util.HashMap;
import java.util.Map;


public class RandomDataGenerator implements InputOperator {
    public final transient DefaultOutputPort<KeyValPair<String, Object>> outport = new DefaultOutputPort<KeyValPair<String, Object>>();
    private HashMap<String, Integer> dataInfo;

    public RandomDataGenerator() {
        this.dataInfo = new HashMap<String, Integer>();
        this.dataInfo.put("a", 1000);
        this.dataInfo.put("b", 1000);
        this.dataInfo.put("c", 1000);
        this.dataInfo.put("d", 1000);
    }

    @Override
    public void emitTuples() {
        for(Map.Entry<String, Integer> e : dataInfo.entrySet()) {
            String key = e.getKey();
            int count = e.getValue();
            for(int i = 0; i < count; ++i) {
                outport.emit(new KeyValPair<String, Object>(key, i));
            }
        }
    }

    @Override
    public void beginWindow(long l) {

    }

    @Override
    public void endWindow() {

    }

    @Override
    public void setup(Context.OperatorContext operatorContext) {

    }

    @Override
    public void teardown() {

    }
}
