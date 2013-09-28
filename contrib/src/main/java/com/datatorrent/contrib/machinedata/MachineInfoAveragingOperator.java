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
package com.datatorrent.contrib.machinedata;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.contrib.machinedata.operator.averaging.AverageData;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.lib.util.TimeBucketKey;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class MachineInfoAveragingOperator extends BaseOperator {

    private Map<TimeBucketKey, List<Map<String, AverageData>>> dataMap =
            new HashMap<TimeBucketKey, List<Map<String, AverageData>>>();

    public final transient DefaultOutputPort<KeyValPair<TimeBucketKey, Map<String, Double>>> outputPort =
            new DefaultOutputPort<KeyValPair<TimeBucketKey, Map<String, Double>>>();

    public transient DefaultOutputPort<String> smtpAlert = new DefaultOutputPort<String>();

    private int threshold = 70;
    private transient DateFormat dateFormat = new SimpleDateFormat();

    private KeyValPair<TimeBucketKey, Map<String, AverageData>> alertTuple = null;
    private boolean procdAlert = false;


    /**
     * Buffer all the tuples as is till end window gets called
     */
    public final transient DefaultInputPort<KeyValPair<TimeBucketKey, Map<String, AverageData>>> inputPort =
        new DefaultInputPort<KeyValPair<TimeBucketKey, Map<String, AverageData>>>() {

        @Override
        public void process(KeyValPair<TimeBucketKey, Map<String, AverageData>> tuple) {
          Calendar time = tuple.getKey().getTime();
          addTuple(tuple);
          if ((alertTuple != null) && !procdAlert) {
            alertTuple.getKey().setTime(time);
            addTuple(alertTuple);
            alertTuple = null;
            procdAlert = true;
          }
        }
    };

    public final transient DefaultInputPort<KeyValPair<TimeBucketKey, Map<String, AverageData>>> alertPort =
        new DefaultInputPort<KeyValPair<TimeBucketKey, Map<String, AverageData>>>() {

            @Override
            public void process(KeyValPair<TimeBucketKey, Map<String, AverageData>> tuple) {
              //addTuple(tuple);
              alertTuple = tuple;
            }
    };

	private void addTuple(KeyValPair<TimeBucketKey, Map<String, AverageData>> tuple) {
        TimeBucketKey key = tuple.getKey();
        List<Map<String, AverageData>> list = dataMap.get(key);
        if (list == null) {
            list = new ArrayList<Map<String, AverageData>>();
            dataMap.put(key, list);
        }
        list.add(tuple.getValue());
	}

    @Override
    public void endWindow() {
        for (Map.Entry<TimeBucketKey, List<Map<String, AverageData>>> entry: dataMap.entrySet()) {
            TimeBucketKey key = entry.getKey();
            List<Map<String, AverageData>> list = entry.getValue();
            Map<String, AverageData> averageResultMap = new HashMap<String, AverageData>();
            for (Map<String, AverageData> map: list) {
                prepareAverageResult(map, "cpu", averageResultMap);
                prepareAverageResult(map, "ram", averageResultMap);
                prepareAverageResult(map, "hdd", averageResultMap);
            }
            Map<String, Double> averageResult = new HashMap<String, Double>();
            for (Map.Entry<String, AverageData> dataEntry: averageResultMap.entrySet()) {
                String valueKey = dataEntry.getKey();
                double average = dataEntry.getValue().getSum() / dataEntry.getValue().getCount();
                averageResult.put(valueKey, average);
                if (average > threshold) {
                  BigDecimal bd = new BigDecimal(average);
                  bd = bd.setScale(2, BigDecimal.ROUND_HALF_UP);
                  String stime = dateFormat.format(key.getTime().getTime());
                  String skey = getKeyInfo(key);
                  //smtpAlert.emit(valueKey + " alert for " + key + " " + valueKey + " usage breached current usage: " + bd.doubleValue() + "% threshold: " + threshold + "%" );
                  //smtpAlert.emit(valueKey.toUpperCase() + " alert at " + stime + " for " + skey + " " + valueKey + " usage breached current usage: " + bd.doubleValue() + "% threshold: " + threshold + "%" );
                  smtpAlert.emit(valueKey.toUpperCase() + " alert at " + stime + " " + valueKey + " usage breached current usage: " + bd.doubleValue() + "% threshold: " + threshold + "%\n\n" + skey );
                }
            }
            outputPort.emit(new KeyValPair<TimeBucketKey, Map<String, Double>>(key, averageResult));
        }
        dataMap.clear();
        procdAlert = false;
    }

    private void prepareAverageResult(Map<String, AverageData> map, String valueKey, Map<String, AverageData> averageResultMap) {
        AverageData average = averageResultMap.get(valueKey);
        if (average == null) {
            average = new AverageData(map.get(valueKey).getSum(),
                    map.get(valueKey).getCount());
            averageResultMap.put(valueKey,average);
        } else {
            average.setSum(average.getSum() + map.get(valueKey).getSum());
            average.setCount(average.getCount() + map.get(valueKey).getCount());
        }
    }

    private String getKeyInfo(TimeBucketKey key) {
      StringBuilder sb = new StringBuilder();
      if (key instanceof MachineKey) {
          MachineKey mkey = (MachineKey)key;
          Integer customer = mkey.getCustomer();
          if (customer != null) {
            sb.append( "customer: " + customer + "\n" );
          }
          Integer product = mkey.getProduct();
          if (product != null) {
            sb.append( "product version: " + product + "\n" );
          }
          Integer os = mkey.getOs();
          if (os != null) {
            sb.append( "os version: " + os + "\n" );
          }
          Integer software1 = mkey.getSoftware1();
          if (software1 != null) {
            sb.append( "software1 version: " + software1 + "\n" );
          }
          Integer software2 = mkey.getSoftware2();
          if (software2 != null) {
            sb.append( "software2 version: " + software2 + "\n" );
          }
          Integer software3 = mkey.getSoftware3();
          if (software3 != null) {
            sb.append( "software3 version: " + software3 + "\n" );
          }
      }
      return sb.toString();
    }
}

