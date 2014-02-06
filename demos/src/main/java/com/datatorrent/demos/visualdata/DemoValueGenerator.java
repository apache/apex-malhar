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
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.io.WidgetOutputOperator.TimeSeriesData;

/**
 * Random value generator for demo charts and widgets
 *
 * @since 0.9.3
 */
public class DemoValueGenerator extends BaseOperator implements InputOperator {
    private int value1 = 50;
    private int value2 = 50;
    private int randomIncrement = 5;
    
    private int randomIncrement2 = 5;
    
    private final static transient Random rand = new Random();
    
    private int progress = 0;
    
    private transient Map<String, Number> topNKeyValues = new HashMap<String, Number>();
    
    private transient HashMap<String, Number> piechartValues = new HashMap<String, Number>();
    
    {
      for (int i = 0; i < 20; i++) {
        if (i < 10){
          topNKeyValues.put("testkey0" + i, 0);
        }
        else{
          topNKeyValues.put("testkey" + i, 0);
        }
      }
    }
    

    @OutputPortFieldAnnotation(name="simple output", optional=false)
    public final transient DefaultOutputPort<TimeSeriesData[]> simpleOutput = new DefaultOutputPort<TimeSeriesData[]>();
    
    @OutputPortFieldAnnotation(name="simple output2", optional=false)
    public final transient DefaultOutputPort<TimeSeriesData[]> simpleOutput2 = new DefaultOutputPort<TimeSeriesData[]>();
    
    @OutputPortFieldAnnotation(name="top 10 output", optional=false)
    public final transient DefaultOutputPort<HashMap<String, Number> > top10Output = new DefaultOutputPort<HashMap<String, Number>>();
    
    @OutputPortFieldAnnotation(name="percentage output", optional=false)
    public final transient DefaultOutputPort<Integer> percentageOutput = new DefaultOutputPort<Integer>();
    
    @OutputPortFieldAnnotation(name="progress output", optional=false)
    public final transient DefaultOutputPort<Integer> progressOutput = new DefaultOutputPort<Integer>();
    
    @OutputPortFieldAnnotation(name="piechart output", optional=false)
    public final transient DefaultOutputPort<HashMap<String, Number>> pieChartOutput = new DefaultOutputPort<HashMap<String, Number>>();

    public DemoValueGenerator() {
    }

    @Override
    public void beginWindow(long windowId) {
    }

    @Override
    public void endWindow() {
        value1 = nextValue(value1, randomIncrement);
        value2 = nextValue(value2, randomIncrement2);
        long time = System.currentTimeMillis();
        
        for (Entry<String, Number> hV : topNKeyValues.entrySet()) {
          hV.setValue(rand.nextInt(10000));
        }
        
        TreeSet<Entry<String, Number>> tSet = new TreeSet<Entry<String,Number>>(new Comparator<Entry<String,Number>>() {
          @Override
          public int compare(Entry<String, Number> o1, Entry<String, Number> o2)
          {
            return (int) (o1.getValue().doubleValue() - o2.getValue().doubleValue() == 0 ? -1 : o1.getValue().doubleValue() - o2.getValue().doubleValue());
          }
        });
        
        tSet.addAll(topNKeyValues.entrySet());
        HashMap<String, Number> topNResult = new HashMap<String, Number>();
        int j = 0;
        for (Entry<String, Number> entry : tSet) {
          if(j++ ==10){
            break;
          }
          topNResult.put(entry.getKey(), entry.getValue());
        }
        
        int[] pieNumbers = {rand.nextInt(100), rand.nextInt(100), rand.nextInt(100), rand.nextInt(100), rand.nextInt(100)};
        int sum = 0;
        for (int i = 0; i < pieNumbers.length; i++) {
          sum += pieNumbers[i];
        }
        for (int i = 0; i < pieNumbers.length; i++) {
          piechartValues.put("part" + i, (double)pieNumbers[i]/(double)sum);
        }
        
        pieChartOutput.emit(piechartValues);
        top10Output.emit(topNResult);
        percentageOutput.emit(rand.nextInt(100));
        progress = (progress + rand.nextInt(5))%100;
        progressOutput.emit(progress);
        TimeSeriesData[] data1 = new TimeSeriesData[1];
        TimeSeriesData adata1 = new TimeSeriesData();
        adata1.time = time; adata1.data = value1;
        data1[0] = adata1;
        TimeSeriesData[] data2 = new TimeSeriesData[1];
        TimeSeriesData adata2 = new TimeSeriesData();
        adata2.time = time; adata2.data = value2;
        data2[0] = adata2;
        simpleOutput.emit(data1);
        simpleOutput2.emit(data2);
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
