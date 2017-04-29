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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Common utility class that can be used by all other operators to handle user input
 * captured from the Web socket input port.
 *
 * @since 0.9.0
 */
public class MerchantTransactionInputHandler extends BaseOperator
{
  public static final String KEY_BANK_ID_NUMBER = "bankIdNum"; // first 12 digits
  public static final String KEY_CREDIT_CARD_NUMBER = "ccNum"; // last 4 digits
  public static final String KEY_MERCHANT_ID = "merchantId";
  public static final String KEY_TERMINAL_ID = "terminalId";
  public static final String KEY_ZIP_CODE = "zipCode";
  public static final String KEY_AMOUNT = "amount";
  public transient DefaultOutputPort<MerchantTransaction> txOutputPort =
      new DefaultOutputPort<MerchantTransaction>();
  public transient DefaultInputPort<Map<String, String>> userTxInputPort = new DefaultInputPort<Map<String, String>>()
  {
    @Override
    public void process(Map<String, String> tuple)
    {
      try {
        txOutputPort.emit(processInput(tuple));
      } catch (Exception exc) {
        logger.error("Exception while handling the input", exc);
      }
    }

  };

  public MerchantTransaction processInput(Map<String, String> tuple)
  {
    String bankIdNum = null;
    String ccNum = null;
    String merchantId = null;
    Integer terminalId = null;
    Integer zipCode = null;
    Long amount = null;
    for (Map.Entry<String, String> e : tuple.entrySet()) {
      if (e.getKey().equals(KEY_BANK_ID_NUMBER)) {
        bankIdNum = e.getValue();
      }
      if (e.getKey().equals(KEY_CREDIT_CARD_NUMBER)) {
        ccNum = e.getValue();
      }
      if (e.getKey().equals(KEY_MERCHANT_ID)) {
        merchantId = e.getValue();
      }
      if (e.getKey().equals(KEY_TERMINAL_ID)) {
        terminalId = new Integer(e.getValue());
      }
      if (e.getKey().equals(KEY_ZIP_CODE)) {
        zipCode = new Integer(e.getValue());
      }
      if (e.getKey().equals(KEY_AMOUNT)) {
        amount = new Long(e.getValue());
      }
    }

    if (bankIdNum == null || ccNum == null || merchantId == null || terminalId == null || zipCode == null || amount == null) {
      throw new IllegalArgumentException("Missing required input!");
    }

    MerchantTransaction tx = new MerchantTransaction();
    tx.bankIdNum = bankIdNum;
    tx.ccNum = ccNum;
    tx.fullCcNum = bankIdNum + " " + ccNum;
    tx.merchantId = merchantId;
    tx.terminalId = terminalId;
    tx.zipCode = zipCode;
    tx.country = "USA";
    tx.amount = amount;
    tx.merchantType = tx.merchantId.equalsIgnoreCase(MerchantTransactionGenerator.merchantIds[2])
            || tx.merchantId.equalsIgnoreCase(MerchantTransactionGenerator.merchantIds[3])
                      ? MerchantTransaction.MerchantType.INTERNET
                      : MerchantTransaction.MerchantType.BRICK_AND_MORTAR;
    tx.transactionType = MerchantTransaction.TransactionType.POS;

    tx.userGenerated = true;
    tx.time = System.currentTimeMillis();

    return tx;

  }

  private static final Logger logger = LoggerFactory.getLogger(MerchantTransactionInputHandler.class);
}
