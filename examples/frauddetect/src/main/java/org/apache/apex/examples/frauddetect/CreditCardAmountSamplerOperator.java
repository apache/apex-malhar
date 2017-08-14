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
import java.util.Iterator;
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
 * An operator to alert in case a transaction of a small lowAmount is followed by a transaction which is significantly larger for a given credit card number.
 * This is done for each transaction. This also means that this happens for each individual credit card.
 * It accepts merchant transaction object and for each CC listed in the transaction(s), checks for the transaction amounts. An alert is raised if the transaction
 * lowAmount is significantly > the lowest amt in this window.
 *
 * @since 0.9.0
 */
public class CreditCardAmountSamplerOperator extends BaseOperator
{
  private final transient JsonFactory jsonFactory = new JsonFactory();
  private final transient ObjectMapper mapper = new ObjectMapper(jsonFactory);
  private static final Logger logger = LoggerFactory.getLogger(Application.class);
  // Factor to be applied to existing lowAmount to flag potential alerts.
  private double threshold = 9500;
  private Map<String, CreditCardInfo> ccTxnMap = new HashMap<String, CreditCardInfo>();
  //private Map<String, MutableLong> ccQueryTxnMap = new HashMap<String, MutableLong>();
  private List<CreditCardAlertData> alerts = new ArrayList<CreditCardAlertData>();
  //private List<CreditCardAlertData> userAlerts = new ArrayList<CreditCardAlertData>();
  private static final String ALERT_MSG =
      "Potential fraudulent CC transactions (small one USD %d followed by large USD %d) performed using credit card: %s";
  public final transient DefaultOutputPort<String> ccAlertOutputPort = new DefaultOutputPort<String>();
  /*
   public final transient DefaultOutputPort<Map<String, Object>> ccUserAlertOutputPort = new DefaultOutputPort<Map<String, Object>>();
   */
  public final transient DefaultOutputPort<Map<String, Object>> ccAlertNotificationPort = new DefaultOutputPort<Map<String, Object>>();

  public double getThreshold()
  {
    return threshold;
  }

  public void setThreshold(double threshold)
  {
    this.threshold = threshold;
  }

  private void processTuple(KeyValPair<MerchantKey, CreditCardData> tuple, Map<String, CreditCardInfo> txMap)
  {
    String fullCcNum = tuple.getValue().fullCcNum;
    long ccAmount = tuple.getValue().amount;
    MerchantKey key = tuple.getKey();

    CreditCardInfo cardInfo = txMap.get(fullCcNum);

    if (cardInfo != null) {
      long currentSmallValue = cardInfo.lowAmount.longValue();
      if (ccAmount < currentSmallValue) {
        cardInfo.lowAmount.setValue(ccAmount);
        cardInfo.time = key.time;
      } else if (ccAmount > (currentSmallValue + threshold)) {
        // If the transaction lowAmount is > 70% of the min. lowAmount, send an alert.

        CreditCardAlertData data = new CreditCardAlertData();

        data.merchantId = key.merchantId;
        data.terminalId = key.terminalId == null ? 0 : key.terminalId;
        data.zipCode = key.zipCode;
        data.merchantType = key.merchantType;
        data.fullCcNum = fullCcNum;
        data.small = currentSmallValue;
        data.large = ccAmount;
        data.threshold = threshold;
        data.userGenerated = key.userGenerated;
        data.time = System.currentTimeMillis();

        alerts.add(data);

        /*
         if (userGenerated){
         userAlerts.add(data);
         }
         */
        ccAlertNotificationPort.emit(getOutputData(data));

        // Any high value transaction after a low value transaction with difference greater than threshold
        // will trigger the alert. Not resetting the low value also helps in a system generated transaction
        // alert not resetting the low value from a user generated transaction
        //txMap.remove(fullCcNum);
      }
    } else {
      cardInfo = new CreditCardInfo();
      cardInfo.lowAmount.setValue(ccAmount);
      cardInfo.time = key.time;
      txMap.put(fullCcNum, cardInfo);
    }
  }

  public transient DefaultInputPort<KeyValPair<MerchantKey, CreditCardData>> inputPort =
      new DefaultInputPort<KeyValPair<MerchantKey, CreditCardData>>()
  {
    //
    // This function checks if a CC entry exists.
    // If so, it checks whether the current transaction is for an lowAmount lesser than the one stored in the hashmap. If so, this becomes the min. transaction lowAmount.
    // If the lowAmount is > 70% of the existing lowAmount in the hash map, raise an alert.
    //
    @Override
    public void process(KeyValPair<MerchantKey, CreditCardData> tuple)
    {

      processTuple(tuple, ccTxnMap);

    }

  };

  @Override
  public void endWindow()
  {

    for (CreditCardAlertData data : alerts) {
      try {
        ccAlertOutputPort.emit(JsonUtils.toJson(data));
      } catch (IOException e) {
        logger.warn("Exception while converting object to JSON", e);
      }
    }

    //for (CreditCardAlertData data: userAlerts) {
       /*for (CreditCardAlertData data: alerts) {
     ccAlertNotificationPort.emit(getOutputData(data));
     }*/

    long ctime = System.currentTimeMillis();
    Iterator<Map.Entry<String, CreditCardInfo>> iterator = ccTxnMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, CreditCardInfo> entry = iterator.next();
      long time = entry.getValue().time;
      if ((ctime - time) > 60000) {
        iterator.remove();
      }
    }

    //ccTxnMap.clear();
    alerts.clear();

    //ccQueryTxnMap.clear();
    //userAlerts.clear();
  }

  private static class CreditCardInfo
  {
    MutableLong lowAmount = new MutableLong();
    Long time;
  }

  private Map<String, Object> getOutputData(CreditCardAlertData data)
  {
    Map<String, Object> output = new HashMap<String, Object>();
    output.put("message", String.format(ALERT_MSG, data.small, data.large, data.fullCcNum));
    output.put("alertType", "smallThenLarge");
    output.put("userGenerated", "" + data.userGenerated);
    output.put("alertData", data);

    try {
      String str = mapper.writeValueAsString(output);
      logger.debug("Alert generated: " + str + " userGenerated: " + data.userGenerated);
    } catch (Exception exc) {
      //ignore
    }

    return output;
  }

}
