/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.examples.frauddetect;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.examples.frauddetect.util.JsonUtils;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.commons.lang.mutable.MutableLong;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Count the transactions for the underlying aggregation window if the same BIN is
 * being used for more than defined number of transactions. Output the data as needed
 * by Mongo output operator
 *
 * @since 0.9.0
 */
public class BankIdNumberSamplerOperator extends BaseOperator
{
  private final transient JsonFactory jsonFactory = new JsonFactory();
  private final transient ObjectMapper mapper = new ObjectMapper(jsonFactory);
  private int threshold;
  private Map<MerchantKey, Map<String, BankIdNumData>> bankIdNumCountMap = new HashMap<MerchantKey, Map<String, BankIdNumData>>();
  private static final String ALERT_MSG =
      "Potential fraudulent CC transactions (same bank id %s and merchant %s) total transactions: %d";
  /**
   * Output the key-value pair for the BIN as key with the count as value.
   */
  public final transient DefaultOutputPort<String> countAlertOutputPort =
      new DefaultOutputPort<String>();
  public final transient DefaultOutputPort<Map<String, Object>> countAlertNotificationPort =
      new DefaultOutputPort<Map<String, Object>>();

  public int getThreshold()
  {
    return threshold;
  }

  public void setThreshold(int threshold)
  {
    this.threshold = threshold;
  }

  /*
  public final transient DefaultInputPort<KeyValPair<MerchantKey, String>> txInputPort =
          new DefaultInputPort<KeyValPair<MerchantKey, String>>()
  {
    @Override
    public void process(KeyValPair<MerchantKey, String> tuple)
    {
      processTuple(tuple);
    }

  };

  private void processTuple(KeyValPair<MerchantKey, String> tuple)
  {
    MerchantKey key = tuple.getKey();
    Map<String, BankIdNumData> map = bankIdNumCountMap.get(key);
    if (map == null) {
      map = new HashMap<String, BankIdNumData>();
      bankIdNumCountMap.put(key, map);
    }
    String bankIdNum = tuple.getValue();
    BankIdNumData bankIdNumData = map.get(bankIdNum);
    if (bankIdNumData == null) {
      bankIdNumData = new BankIdNumData();
      bankIdNumData.bankIdNum = bankIdNum;
      map.put(bankIdNum, bankIdNumData);
    }
    bankIdNumData.count.increment();
    if (key.userGenerated) {
      bankIdNumData.userGenerated = true;
    }
  }
  */

  public final transient DefaultInputPort<KeyValPair<KeyValPair<MerchantKey, String>, Integer>> txCountInputPort =
      new DefaultInputPort<KeyValPair<KeyValPair<MerchantKey, String>, Integer>>()
  {
    @Override
    public void process(KeyValPair<KeyValPair<MerchantKey, String>, Integer> tuple)
    {
      processTuple(tuple.getKey(), tuple.getValue());
    }

  };

  private void processTuple(KeyValPair<MerchantKey, String> tuple, Integer count)
  {
    MerchantKey key = tuple.getKey();
    Map<String, BankIdNumData> map = bankIdNumCountMap.get(key);
    if (map == null) {
      map = new HashMap<String, BankIdNumData>();
      bankIdNumCountMap.put(key, map);
    }
    String bankIdNum = tuple.getValue();
    BankIdNumData bankIdNumData = map.get(bankIdNum);
    if (bankIdNumData == null) {
      bankIdNumData = new BankIdNumData();
      bankIdNumData.bankIdNum = bankIdNum;
      map.put(bankIdNum, bankIdNumData);
    }
    bankIdNumData.count.setValue(count);
    if (key.userGenerated) {
      bankIdNumData.userGenerated = true;
    }
  }

  /**
   * Go through the BIN Counter map and check if any of the values for the BIN exceed the threshold.
   * If yes, generate the alert on the output port.
   */
  @Override
  public void endWindow()
  {
    for (Map.Entry<MerchantKey, Map<String, BankIdNumData>> entry : bankIdNumCountMap.entrySet()) {
      List<BankIdNumData> list = null;
      MerchantKey key = entry.getKey();
      if (key.merchantType == MerchantTransaction.MerchantType.INTERNET) {
        continue;
      }
      list = dataOutput(entry.getValue());
      if (list.size() > 0) {
        for (BankIdNumData binData : list) {
          BankIdNumberAlertData data = new BankIdNumberAlertData();
          data.merchantId = key.merchantId;
          data.terminalId = key.terminalId == null ? 0 : key.terminalId;
          data.zipCode = key.zipCode;
          data.merchantType = key.merchantType;
          data.bankIdNum = binData.bankIdNum;
          data.count = binData.count.intValue();
          data.userGenerated = binData.userGenerated;
          data.time = System.currentTimeMillis();
          try {
            countAlertOutputPort.emit(JsonUtils.toJson(data));
            countAlertNotificationPort.emit(getOutputData(data));
          } catch (IOException e) {
            logger.warn("Exception while converting object to JSON: ", e);
          }
        }
      }
    }
    bankIdNumCountMap.clear();
  }

  private List<BankIdNumData> dataOutput(Map<String, BankIdNumData> map)
  {
    List<BankIdNumData> list = new ArrayList<BankIdNumData>();
    int count = 0;
    for (Map.Entry<String, BankIdNumData> bankIdEntry : map.entrySet()) {
      BankIdNumData data = bankIdEntry.getValue();
      if (data.count.intValue() > threshold) {
        list.add(data);
      }
    }
    return list;
  }

  private Map<String, Object> getOutputData(BankIdNumberAlertData data)
  {
    Map<String, Object> output = new HashMap<String, Object>();
    output.put("message", String.format(ALERT_MSG, data.bankIdNum, data.merchantId, data.count));
    output.put("alertType", "sameBankId");
    output.put("userGenerated", "" + data.userGenerated);
    output.put("alertData", data);

    try {
      String str = mapper.writeValueAsString(output);
      logger.debug("user generated tx alert: " + str);
    } catch (Exception exc) {
      //ignore
    }

    return output;
  }

  public static final class BankIdNumData
  {
    public String bankIdNum;
    public MutableLong count = new MutableLong();
    public boolean userGenerated = false;
  }

  private static final Logger logger = LoggerFactory.getLogger(BankIdNumberSamplerOperator.class);
}
