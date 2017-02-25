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
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.apex.examples.frauddetect.util.JsonUtils;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

import com.datatorrent.common.util.BaseOperator;

/**
 * Information tuple generator with randomness.
 *
 * @since 0.9.0
 */
public class MerchantTransactionGenerator extends BaseOperator implements InputOperator
{
  private final Random randomNum = new Random();
  public static final int[] zipCodes = {94086, 94087, 94088, 94089, 94090, 94091, 94092, 94093};
  public static final String[] merchantIds = {"Wal-Mart", "Target", "Amazon", "Apple", "Sears", "Macys", "JCPenny", "Levis"};
//    public static final String bankIdNums[] = { "1111 1111 1111", "2222 2222 2222", "3333 3333 3333", "4444 4444 4444", "5555 5555 5555", "6666 6666 6666", "7777 7777 7777", "8888 8888 8888"};
//    public static final String ccNums[] = { "0001", "0002", "0003", "0004", "0005", "0006", "0007", "0008"};
//    public static final String bankIdNums[] = { "1111 1111 1111", "2222 2222 2222", "3333 3333 3333", "4444 4444 4444"};
//    public static final String ccNums[] = { "0001", "0002", "0003", "0004"};
//    public static final int zipCodes[] = { 94086, 94087, 94088, 94089, 94090};
//    public static final String merchantIds[] = { "Wal-Mart", "Target", "Amazon", "Apple"};
//    private int bankIdNumMin = 0;
//    private int bankIdNumMax = bankIdNums.length - 1;
//    private int ccMin = 0;
//    private int ccMax = ccNums.length - 1;
  private int amountMin = 1;
  private int amountMax = 400;
  private int merchantIdMin = 0;
  private int merchantIdMax = merchantIds.length - 1;
  private int terminalIdMin = 1;
  private int terminalIdMax = 8;
  private int zipMin = 0;
  private int zipMax = zipCodes.length - 1;
  private int tupleBlastSize = 2000;
  private boolean stopGeneration = false;

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
  }

  public transient DefaultOutputPort<MerchantTransaction> txOutputPort =
      new DefaultOutputPort<MerchantTransaction>();
  public transient DefaultOutputPort<String> txDataOutputPort =
      new DefaultOutputPort<String>();

  @Override
  public void emitTuples()
  {
    int count = 0;
    List<MerchantTransaction> txList = new ArrayList();

    while (!stopGeneration && count < getTupleBlastSize()) {

      String bankIdNum = genBankIdNum();
      String ccNum = genCC();
      int merchant = genMerchantId();
      int terminal = genTerminalId();
      int zip = genZip();

      long amount = genAmount();

//            int bankIdNum = 1;
//            int ccNum = 2;
//            long amount = 5000;
//            int merchant = 3;
//            int terminal = 4;
//            int zip = 0;

      MerchantTransaction tx = new MerchantTransaction();
      tx.bankIdNum = bankIdNum;
      tx.ccNum = ccNum;
      tx.fullCcNum = tx.bankIdNum + " " + tx.ccNum;
      tx.amount = amount;
      tx.merchantId = merchantIds[merchant];

      // its INTERNET merchant
      tx.merchantType = merchant == 2 || merchant == 3
                        ? MerchantTransaction.MerchantType.INTERNET
                        : MerchantTransaction.MerchantType.BRICK_AND_MORTAR;

      tx.transactionType = MerchantTransaction.TransactionType.POS;

      // set terminal only for a BRICK_AND_MORTAR merchant
      if (merchant != 2 && merchant != 3) {
        tx.terminalId = terminal;
      }
      tx.zipCode = zipCodes[zip];
      tx.country = "USA";
      tx.time = System.currentTimeMillis();

      tx.userGenerated = false;

      txOutputPort.emit(tx);

      txList.add(tx);

      count++;
    }
    for (MerchantTransaction txData : txList) {
      try {
        txDataOutputPort.emit(JsonUtils.toJson(txData));
      } catch (IOException e) {
        logger.warn("Exception while converting object to JSON", e);
      }
    }
    txList.clear();

    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }

  public String genBankIdNum()
  {
    // Bank ID will be between 1000 0000 and 3500 0000 (25 BINs)
    int base = randomNum.nextInt(100) + 100;
    return base + "0 0000";
  }

  public String genCC()
  {
    // CC will be 1000 0000 to 1400 0000 (400,000 cards per BIN)
    int base = (randomNum.nextInt(100000) + 10000000);
    String baseString = Integer.toString(base);
    return baseString.substring(0, 4) + " " + baseString.substring(4);
  }

  public int genAmount()
  {
    int lowRange = 50;
    int range = amountMax - amountMin + randomNum.nextInt(lowRange);
    return amountMin + randomNum.nextInt(range);
  }

  public int genMerchantId()
  {
    int range = merchantIdMax - merchantIdMin + 1;
    return merchantIdMin + randomNum.nextInt(range);
  }

  public int genTerminalId()
  {
    int range = terminalIdMax - terminalIdMin + 1;
    return terminalIdMin + randomNum.nextInt(range);
  }

  public int genZip()
  {
    int range = zipMax - zipMin + 1;
    return zipMin + randomNum.nextInt(range);
  }

  public void setStopGeneration(boolean stopGeneration)
  {
    this.stopGeneration = stopGeneration;
  }

  /**
   * @return the tupleBlastSize
   */
  public int getTupleBlastSize()
  {
    return tupleBlastSize;
  }

  /**
   * @param tupleBlastSize the tupleBlastSize to set
   */
  public void setTupleBlastSize(int tupleBlastSize)
  {
    this.tupleBlastSize = tupleBlastSize;
  }

  private static final Logger logger = LoggerFactory.getLogger(MerchantTransactionGenerator.class);
}
