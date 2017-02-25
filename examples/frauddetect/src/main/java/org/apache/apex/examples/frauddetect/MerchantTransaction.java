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

import java.io.Serializable;

/**
 * POJO for BIN Alert related data.
 *
 * @since 0.9.0
 */
public class MerchantTransaction implements Serializable
{
  public enum MerchantType
  {
    UNDEFINED, BRICK_AND_MORTAR, INTERNET
  }

  public enum TransactionType
  {
    UNDEFINED, POS
  }

  public String ccNum;
  public String bankIdNum;
  public String fullCcNum;
  public Long amount;
  public String merchantId;
  public Integer terminalId;
  public Integer zipCode;
  public String country;
  public MerchantType merchantType = MerchantType.UNDEFINED;
  public TransactionType transactionType = TransactionType.UNDEFINED;
  public Long time;
  public boolean userGenerated;

  public MerchantTransaction()
  {
  }

  @Override
  public int hashCode()
  {
    int key = 0;
    if (ccNum != null) {
      key |= (1 << 1);
      key |= (ccNum.hashCode());
    }
    if (bankIdNum != null) {
      key |= (1 << 2);
      key |= (bankIdNum.hashCode());
    }
    if (amount != null) {
      key |= (1 << 6);
      key |= (amount << 4);
    }
    if (merchantId != null) {
      key |= (1 << 3);
      key |= (merchantId.hashCode());
    }
    if (terminalId != null) {
      key |= (1 << 4);
      key |= (terminalId << 2);
    }
    if (zipCode != null) {
      key |= (1 << 5);
      key |= (zipCode << 3);
    }
    if (country != null) {
      key |= (1 << 7);
      key |= (country.hashCode());
    }
    if (merchantType != null) {
      key |= (1 << 8);
      key |= (merchantType.hashCode());
    }
    if (transactionType != null) {
      key |= (1 << 9);
      key |= (transactionType.hashCode());
    }
    if (fullCcNum != null) {
      key |= (1 << 10);
      key |= (fullCcNum.hashCode());
    }
    if (time != null) {
      key |= (1 << 11);
      key |= (time << 2);
    }

    return key;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (!(obj instanceof MerchantTransaction)) {
      return false;
    }
    MerchantTransaction mtx = (MerchantTransaction)obj;
    return checkStringEqual(this.ccNum, mtx.ccNum)
            && checkStringEqual(this.bankIdNum, mtx.bankIdNum)
            && checkLongEqual(this.amount, mtx.amount)
            && checkStringEqual(this.merchantId, mtx.merchantId)
            && checkIntEqual(this.terminalId, mtx.terminalId)
            && checkIntEqual(this.zipCode, mtx.zipCode)
            && checkStringEqual(this.country, mtx.country)
            && checkIntEqual(this.merchantType.ordinal(), mtx.merchantType.ordinal())
            && checkIntEqual(this.transactionType.ordinal(), mtx.transactionType.ordinal())
            && checkStringEqual(this.fullCcNum, mtx.fullCcNum)
            && checkLongEqual(this.time, mtx.time);
  }

  private boolean checkIntEqual(Integer a, Integer b)
  {
    if ((a == null) && (b == null)) {
      return true;
    }
    if ((a != null) && (b != null) && a.intValue() == b.intValue()) {
      return true;
    }
    return false;
  }

  private boolean checkLongEqual(Long a, Long b)
  {
    if ((a == null) && (b == null)) {
      return true;
    }
    if ((a != null) && (b != null) && a.longValue() == b.longValue()) {
      return true;
    }
    return false;
  }

  private boolean checkStringEqual(String a, String b)
  {
    if ((a == null) && (b == null)) {
      return true;
    }
    if ((a != null) && a.equals(b)) {
      return true;
    }
    return false;
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    if (ccNum != null) {
      sb.append("|0:").append(ccNum);
    }
    if (bankIdNum != null) {
      sb.append("|1:").append(bankIdNum);
    }
    if (fullCcNum != null) {
      sb.append("|2:").append(fullCcNum);
    }
    if (amount != null) {
      sb.append("|3:").append(amount);
    }
    if (merchantId != null) {
      sb.append("|4:").append(merchantId);
    }
    if (terminalId != null) {
      sb.append("|5:").append(terminalId);
    }
    if (zipCode != null) {
      sb.append("|6:").append(zipCode);
    }
    if (country != null) {
      sb.append("|7:").append(country);
    }
    if (merchantType != null) {
      sb.append("|8:").append(merchantType);
    }
    if (transactionType != null) {
      sb.append("|9:").append(transactionType);
    }
    if (time != null) {
      sb.append("|10:").append(time);
    }
    return sb.toString();
  }

}
