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

/**
 * Common utility class that can be used by all other operators to handle user input
 * captured from the Web socket input port.
 *
 * @since 0.9.0
 */
public class MerchantQueryInputHandler
{
  public static final String KEY_DATA = "data";
  public static final String KEY_MERCHANT_ID = "merchantId";
  public static final String KEY_TERMINAL_ID = "terminalId";
  public static final String KEY_ZIP_CODE = "zipCode";

  public static MerchantKey process(Map<String, Object> tuple)
  {
    String merchantId = null;
    Integer terminalId = null;
    Integer zipCode = null;

    // ignoring other top-level attributes.
    Map<String, Object> data = (Map<String, Object>)tuple.get(KEY_DATA);
    if (data.get(KEY_MERCHANT_ID) != null) {
      merchantId = (String)data.get(KEY_MERCHANT_ID);
    }
    if (data.get(KEY_TERMINAL_ID) != null) {
      terminalId = (Integer)data.get(KEY_TERMINAL_ID);
    }
    if (data.get(KEY_ZIP_CODE) != null) {
      zipCode = (Integer)data.get(KEY_ZIP_CODE);
    }

    MerchantKey key = new MerchantKey();
    key.merchantId = merchantId;
    key.terminalId = terminalId;
    key.zipCode = zipCode;
    key.country = "USA";
    if (merchantId != null) {
      key.merchantType = key.merchantId.equalsIgnoreCase(MerchantTransactionGenerator.merchantIds[2])
              || key.merchantId.equalsIgnoreCase(MerchantTransactionGenerator.merchantIds[3])
                         ? MerchantTransaction.MerchantType.INTERNET
                         : MerchantTransaction.MerchantType.BRICK_AND_MORTAR;
    }
    return key;

  }

}
