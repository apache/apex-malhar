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

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.lang.mutable.MutableLong;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * A bucket-like operator to accept merchant transaction object and dissipate the
 * transaction amount to the further downstream operator for calculating min, max and std-deviation.
 *
 * @since 0.9.0
 */
public class MerchantTransactionBucketOperator extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(MerchantTransactionBucketOperator.class);
  /*
  public final transient DefaultOutputPort<KeyValPair<MerchantKey, String>> binOutputPort =
          new DefaultOutputPort<KeyValPair<MerchantKey, String>>();
  */
  public final transient DefaultOutputPort<KeyValPair<KeyValPair<MerchantKey, String>, Integer>> binCountOutputPort =
      new DefaultOutputPort<KeyValPair<KeyValPair<MerchantKey, String>, Integer>>();
  public final transient DefaultOutputPort<KeyValPair<MerchantKey, Long>> txOutputPort =
      new DefaultOutputPort<KeyValPair<MerchantKey, Long>>();
  public final transient DefaultOutputPort<KeyValPair<MerchantKey, CreditCardData>> ccAlertOutputPort =
      new DefaultOutputPort<KeyValPair<MerchantKey, CreditCardData>>();
  public final transient DefaultOutputPort<Map<String, Object>> summaryTxnOutputPort =
      new DefaultOutputPort<Map<String, Object>>();
  private MutableLong totalTxns = new MutableLong(0);
  private MutableLong txnsInLastSecond = new MutableLong(0);
  private MutableDouble amtInLastSecond = new MutableDouble(0);
  private transient DecimalFormat amtFormatter = new DecimalFormat("#.##");
  public transient DefaultInputPort<MerchantTransaction> inputPort = new DefaultInputPort<MerchantTransaction>()
  {
    @Override
    public void process(MerchantTransaction tuple)
    {
      processTuple(tuple);
    }

  };
  public transient DefaultInputPort<MerchantTransaction> txUserInputPort = new DefaultInputPort<MerchantTransaction>()
  {
    @Override
    public void process(MerchantTransaction tuple)
    {
      processTuple(tuple);
    }

  };

  public void endWindow()
  {
    Map<String, Object> summary = new HashMap<String, Object>();
    double avg;
    if (txnsInLastSecond.longValue() == 0) {
      avg = 0;
    } else {
      avg = amtInLastSecond.doubleValue() / txnsInLastSecond.longValue();
    }
    summary.put("totalTxns", totalTxns);
    summary.put("txnsInLastSecond", txnsInLastSecond);
    summary.put("amtInLastSecond", amtFormatter.format(amtInLastSecond));
    summary.put("avgAmtInLastSecond", amtFormatter.format(avg));
    summaryTxnOutputPort.emit(summary);
    txnsInLastSecond.setValue(0);
    amtInLastSecond.setValue(0);
  }

  private void processTuple(MerchantTransaction tuple)
  {
    emitBankIdNumTuple(tuple, binCountOutputPort);
    emitMerchantKeyTuple(tuple, txOutputPort);
    emitCreditCardKeyTuple(tuple, ccAlertOutputPort);
    totalTxns.increment();
    txnsInLastSecond.increment();
    amtInLastSecond.add(tuple.amount);
  }

  private void emitMerchantKeyTuple(MerchantTransaction tuple, DefaultOutputPort<KeyValPair<MerchantKey, Long>> outputPort)
  {
    MerchantKey key = getMerchantKey(tuple);
    KeyValPair<MerchantKey, Long> keyValPair = new KeyValPair<MerchantKey, Long>(key, tuple.amount);
    outputPort.emit(keyValPair);
  }

  //private void emitBankIdNumTuple(MerchantTransaction tuple, DefaultOutputPort<KeyValPair<MerchantKey, String>> outputPort)
  private void emitBankIdNumTuple(MerchantTransaction tuple, DefaultOutputPort<KeyValPair<KeyValPair<MerchantKey, String>, Integer>> outputPort)
  {
    MerchantKey key = getMerchantKey(tuple);
    KeyValPair<MerchantKey, String> keyValPair = new KeyValPair<MerchantKey, String>(key, tuple.bankIdNum);
    outputPort.emit(new KeyValPair<KeyValPair<MerchantKey, String>, Integer>(keyValPair, 1));
  }

  private void emitCreditCardKeyTuple(MerchantTransaction tuple, DefaultOutputPort<KeyValPair<MerchantKey, CreditCardData>> outputPort)
  {
    MerchantKey key = getMerchantKey(tuple);

    CreditCardData data = new CreditCardData();
    data.fullCcNum = tuple.fullCcNum;
    data.amount = tuple.amount;

    KeyValPair<MerchantKey, CreditCardData> keyValPair = new KeyValPair<MerchantKey, CreditCardData>(key, data);
    outputPort.emit(keyValPair);
  }

  private MerchantKey getMerchantKey(MerchantTransaction tuple)
  {
    MerchantKey key = new MerchantKey();
    key.merchantId = tuple.merchantId;
    key.terminalId = tuple.terminalId;
    key.zipCode = tuple.zipCode;
    key.country = tuple.country;
    key.merchantType = tuple.merchantType;
    key.userGenerated = tuple.userGenerated;
    key.time = tuple.time;
    return key;
  }

}
