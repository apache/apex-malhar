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

package com.datatorrent.demos.visualdata;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeSet;

import org.apache.commons.lang3.RandomStringUtils;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * Chart value generator.
 */
public class DemoValueGenerator extends BaseOperator implements InputOperator {
    private int value1 = 50;
    private int value2 = 50;
    private int randomIncrement = 5;
    
    private int randomIncrement2 = 5;
    
    private final static transient Random rand = new Random();
    
    private transient Map<String, Integer> topNKeyValues = new HashMap<String, Integer>();
    
    {
      for (int i = 0; i < 20; i++) {
        topNKeyValues.put(RandomStringUtils.randomAlphanumeric(rand.nextInt(10) + 5), 0);
      }
    }

    @OutputPortFieldAnnotation(name="simple output", optional=false)
    public final transient DefaultOutputPort<Integer> simpleOutput = new DefaultOutputPort<Integer>();
    
    @OutputPortFieldAnnotation(name="simple output2", optional=false)
    public final transient DefaultOutputPort<Integer> simpleOutput2 = new DefaultOutputPort<Integer>();
    
    @OutputPortFieldAnnotation(name="top 10 output", optional=false)
    public final transient DefaultOutputPort<HashMap<String, Integer>> top10Output = new DefaultOutputPort<HashMap<String,Integer>>();
    
    @OutputPortFieldAnnotation(name="percentage output", optional=false)
    public final transient DefaultOutputPort<Integer> percentageOutput = new DefaultOutputPort<Integer>();

    public DemoValueGenerator() {
    }

    @Override
    public void beginWindow(long windowId) {
    }

    @Override
    public void endWindow() {
        value1 = nextValue(value1, randomIncrement);
        
        for (Entry<String, Integer> hV : topNKeyValues.entrySet()) {
          hV.setValue(rand.nextInt(10000));
        }
        
        TreeSet<Entry<String, Integer>> tSet = new TreeSet<Entry<String,Integer>>(new Comparator<Entry<String,Integer>>() {
          @Override
          public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2)
          {
            return o1.getValue() - o2.getValue() == 0 ? -1 : o1.getValue() - o2.getValue();
          }
        });
        
        tSet.addAll(topNKeyValues.entrySet());
        HashMap<String, Integer> topNResult = new HashMap<String, Integer>();
        int j = 0;
        for (Entry<String, Integer> entry : tSet) {
          if(j++ ==10){
            break;
          }
          topNResult.put(entry.getKey(), entry.getValue());
        }
        
        top10Output.emit(topNResult);
        percentageOutput.emit(rand.nextInt(100));
        simpleOutput.emit(Integer.valueOf(value1));
        simpleOutput.emit(Integer.valueOf(value2));
    }

    @Override
    public void emitTuples() {
    }

    private int nextValue(int oldValue, int randInc) {
        int nextValue = oldValue + (int) (Math.random() * randInc - randInc / 2);
        nextValue = nextValue < 0 ? 0 : nextValue > 100 ? 0 : nextValue;
        return nextValue;
    }
    
    public int getRandomIncrement() {
        return randomIncrement;
    }

    public void setRandomIncrement(int increment) {
        randomIncrement = increment;
    }
    public int getRandomIncrement2() {
        return randomIncrement2;
    }

    public void setRandomIncrement2(int increment2) {
        randomIncrement2 = increment2;
    }
}
