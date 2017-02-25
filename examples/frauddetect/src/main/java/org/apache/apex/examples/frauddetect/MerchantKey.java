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
 * A time-based key for merchant data.
 *
 * @since 0.9.0
 */
public class MerchantKey implements Serializable
{
  public String merchantId;
  public Integer terminalId;
  public Integer zipCode;
  public String country;
  public MerchantTransaction.MerchantType merchantType;
  public Long time;
  public boolean userGenerated;

  public MerchantKey()
  {
  }

  @Override
  public int hashCode()
  {
    int key = 0;
    if (merchantId != null) {
      key |= (1 << 1);
      key |= (merchantId.hashCode());
    }
    if (terminalId != null) {
      key |= (1 << 2);
      key |= (terminalId << 2);
    }
    if (zipCode != null) {
      key |= (1 << 3);
      key |= (zipCode << 3);
    }
    if (country != null) {
      key |= (1 << 4);
      key |= (country.hashCode());
    }
    if (merchantType != null) {
      key |= (1 << 5);
      key |= (merchantType.hashCode());
    }
    return key;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (!(obj instanceof MerchantKey)) {
      return false;
    }
    MerchantKey mkey = (MerchantKey)obj;
    return checkStringEqual(this.merchantId, mkey.merchantId)
            && checkIntEqual(this.terminalId, mkey.terminalId)
            && checkIntEqual(this.zipCode, mkey.zipCode)
            && checkStringEqual(this.country, mkey.country)
            && checkIntEqual(this.merchantType.ordinal(), mkey.merchantType.ordinal());
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
    if (merchantId != null) {
      sb.append("|1:").append(merchantId);
    }
    if (terminalId != null) {
      sb.append("|2:").append(terminalId);
    }
    if (zipCode != null) {
      sb.append("|3:").append(zipCode);
    }
    if (country != null) {
      sb.append("|4:").append(country);
    }
    if (merchantType != null) {
      sb.append("|5:").append(merchantType);
    }
    return sb.toString();
  }

}
