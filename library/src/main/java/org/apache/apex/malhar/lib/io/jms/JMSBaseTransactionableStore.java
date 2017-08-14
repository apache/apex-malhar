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
package org.apache.apex.malhar.lib.io.jms;

import org.apache.apex.malhar.lib.db.TransactionableStore;

/**
 * This is a base implementation for a transactionable store.
 *
 * @since 2.0.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class JMSBaseTransactionableStore implements TransactionableStore
{
  /**
   * JMS base class.
   */
  private transient JMSBase base;
  /**
   * App Id of parent operator.
   */
  private transient String appId;
  /**
   * Operator Id of parent operator.
   */
  private transient int operatorId;

  public JMSBaseTransactionableStore()
  {
  }

  /**
   * This method sets the JMSBase to perform transactions over.
   * @param base The JMSBase to perform transactions over.
   */
  protected void setBase(JMSBase base)
  {
    this.base = base;
  }

  /**
   * This method gets the JMSBase to perform transactions over.
   * @return The JMSBase to perform transactions over.
   */
  protected JMSBase getBase()
  {
    return base;
  }

  /**
   * True if this store actually keeps track of processed windows and performs jms transactions.
   * @return True if this store actually keeps track of processed windows and performs jms transactions.
   */
  protected boolean isTransactable()
  {
    return true;
  }

  /**
   * True if this store supports the exactly once operation mode.
   * @return True if this store supports the exactly once operation mode.
   */
  protected boolean isExactlyOnce()
  {
    return true;
  }

  /**
   * This method sets the application id of the parent operator.
   * @param appId The application id of the parent operator.
   */
  protected void setAppId(String appId)
  {
    this.appId = appId;
  }

  /**
   * This method gets the application id of the parent operator.
   * @return The application id of the parent operator.
   */
  protected String getAppId()
  {
    return appId;
  }

  /**
   * This method sets the operator id of the parent operator.
   * @param operatorId The operator id of the parent operator.
   */
  protected void setOperatorId(int operatorId)
  {
    this.operatorId = operatorId;
  }

  /**
   * This method gets the operator id of the parent operator.
   * @return The operator id of the parent operator.
   */
  protected int getOperatorId()
  {
    return operatorId;
  }
}
