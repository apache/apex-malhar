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
import javax.validation.constraints.NotNull;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.examples.frauddetect.util.JsonUtils;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.commons.lang.mutable.MutableDouble;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Generate an alert if the current transaction amount received on tx input port for the given key is greater by n %
 * than the SMA of the last application window as received on the SMA input port.
 *
 * @since 0.9.0
 */
public class AverageAlertingOperator extends BaseOperator
{
  private static final Logger Log = LoggerFactory.getLogger(AverageAlertingOperator.class);
  private final transient JsonFactory jsonFactory = new JsonFactory();
  private final transient ObjectMapper mapper = new ObjectMapper(jsonFactory);
  private Map<MerchantKey, MutableDouble> lastSMAMap = new HashMap<MerchantKey, MutableDouble>();
  private Map<MerchantKey, MutableDouble> currentSMAMap = new HashMap<MerchantKey, MutableDouble>();
  private List<AverageAlertData> alerts = new ArrayList<AverageAlertData>();
  @NotNull
  private int threshold;
  private static final String brickMortarAlertMsg = "Transaction amount %d exceeded by %f (last SMA %f) for Merchant %s at Terminal %d!";
  private static final String internetAlertMsg = "Transaction amount %d exceeded by %f (last SMA %f) for Merchant %s!";
  public final transient DefaultOutputPort<String> avgAlertOutputPort = new DefaultOutputPort<String>();
  public final transient DefaultOutputPort<Map<String, Object>> avgAlertNotificationPort = new DefaultOutputPort<Map<String, Object>>();
  public final transient DefaultInputPort<KeyValPair<MerchantKey, Double>> smaInputPort =
      new DefaultInputPort<KeyValPair<MerchantKey, Double>>()
  {
    @Override
    public void process(KeyValPair<MerchantKey, Double> tuple)
    {
      MutableDouble currentSma = currentSMAMap.get(tuple.getKey());
      if (currentSma == null) { // first sma for the given key
        double sma = tuple.getValue();
        currentSMAMap.put(tuple.getKey(), new MutableDouble(sma));
        //lastSMAMap.put(tuple.getKey(), new MutableDouble(sma));
      } else { // move the current SMA value to the last SMA Map
        //lastSMAMap.get(tuple.getKey()).setValue(currentSma.getValue());
        currentSma.setValue(tuple.getValue());  // update the current SMA value
      }
    }

  };
  public final transient DefaultInputPort<KeyValPair<MerchantKey, Long>> txInputPort =
      new DefaultInputPort<KeyValPair<MerchantKey, Long>>()
  {
    @Override
    public void process(KeyValPair<MerchantKey, Long> tuple)
    {
      processTuple(tuple);
    }

  };

  private void processTuple(KeyValPair<MerchantKey, Long> tuple)
  {
    MerchantKey merchantKey = tuple.getKey();
    MutableDouble lastSma = lastSMAMap.get(tuple.getKey());
    long txValue = tuple.getValue();
    if (lastSma != null && txValue > lastSma.doubleValue()) {
      double lastSmaValue = lastSma.doubleValue();
      double change = txValue - lastSmaValue;
      if (change > threshold) { // generate an alert
        AverageAlertData data = getOutputData(merchantKey, txValue, change, lastSmaValue);
        alerts.add(data);
        //if (userGenerated) {   // if its user generated only the pass it to WebSocket
        if (merchantKey.merchantType == MerchantTransaction.MerchantType.BRICK_AND_MORTAR) {
          avgAlertNotificationPort.emit(getOutputData(data, String.format(brickMortarAlertMsg, txValue, change, lastSmaValue, merchantKey.merchantId, merchantKey.terminalId)));
        } else { // its internet based
          avgAlertNotificationPort.emit(getOutputData(data, String.format(internetAlertMsg, txValue, change, lastSmaValue, merchantKey.merchantId)));

        }
        //}
      }
    }
  }

  @Override
  public void endWindow()
  {
    for (AverageAlertData data : alerts) {
      try {
        avgAlertOutputPort.emit(JsonUtils.toJson(data));
      } catch (IOException e) {
        logger.warn("Exception while converting object to JSON", e);
      }
    }

    alerts.clear();

    for (Map.Entry<MerchantKey, MutableDouble> entry : currentSMAMap.entrySet()) {
      MerchantKey key = entry.getKey();
      MutableDouble currentSma = entry.getValue();
      MutableDouble lastSma = lastSMAMap.get(key);
      if (lastSma == null) {
        lastSma = new MutableDouble(currentSma.doubleValue());
        lastSMAMap.put(key, lastSma);
      } else {
        lastSma.setValue(currentSma.getValue());
      }
    }
  }

  private AverageAlertData getOutputData(MerchantKey key, long amount, double change, double lastSmaValue)
  {
    AverageAlertData data = new AverageAlertData();

    data.merchantId = key.merchantId;
    data.terminalId = key.terminalId == null ? 0 : key.terminalId;
    data.zipCode = key.zipCode;
    data.merchantType = key.merchantType;
    data.amount = amount;
    data.lastSmaValue = lastSmaValue;
    data.change = change;
    //data.userGenerated = userGenerated;
    data.userGenerated = key.userGenerated;
    data.time = System.currentTimeMillis();

    return data;
  }

  private Map<String, Object> getOutputData(AverageAlertData data, String msg)
  {
    Map<String, Object> output = new HashMap<String, Object>();
    output.put("message", msg);
    output.put("alertType", "aboveAvg");
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

  public int getThreshold()
  {
    return threshold;
  }

  public void setThreshold(int threshold)
  {
    this.threshold = threshold;
  }

  private static final Logger logger = LoggerFactory.getLogger(AverageAlertingOperator.class);
}
