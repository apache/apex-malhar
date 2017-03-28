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


package org.apache.apex.examples.filter;

/**
 * @since 3.7.0
 */
public class TransactionPOJO
{

  private long trasactionId;
  private double amount;
  private long accountNumber;
  private String type;

  public long getTrasactionId()
  {
    return trasactionId;
  }

  public void setTrasactionId(long trasactionId)
  {
    this.trasactionId = trasactionId;
  }

  public double getAmount()
  {
    return amount;
  }

  public void setAmount(double amount)
  {
    this.amount = amount;
  }

  public long getAccountNumber()
  {
    return accountNumber;
  }

  public void setAccountNumber(long accountNumber)
  {
    this.accountNumber = accountNumber;
  }

  public String getType()
  {
    return type;
  }

  public void setType(String type)
  {
    this.type = type;
  }

  @Override
  public String toString()
  {
    return "TransactionPOJO [trasactionId=" + trasactionId + ", amount=" + amount + ", accountNumber=" + accountNumber
        + ", type=" + type + "]";
  }
}
