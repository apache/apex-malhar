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

import org.apache.apex.malhar.lib.util.TimeBucketKey;

/**
 * Bank Id Number Key
 *
 * @since 0.9.0
 */
public class BankIdNumberKey extends TimeBucketKey implements Serializable
{
  public String bankIdNum;

  public BankIdNumberKey()
  {
  }

  @Override
  public int hashCode()
  {
    int key = 0;
    key |= (1 << 1);
    key |= (bankIdNum.hashCode());
    return super.hashCode() ^ key;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (!(obj instanceof BankIdNumberKey)) {
      return false;
    }
    return super.equals(obj)
            && bankIdNum.equals(((BankIdNumberKey)obj).bankIdNum);
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder(super.toString());
    sb.append("|1:").append(bankIdNum);
    return sb.toString();
  }

}
