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
package org.apache.apex.malhar.contrib.hbase;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * A base implementation of an AggregateTransactionableStoreOutputOperator
 * operator for storing tuples in HBase rows and provides a batch put.Subclasses
 * should provide implementation for put operation. <br>
 * <p>
 * <br>
 * This class provides a HBase output operator that can be used to store tuples
 * in rows in a HBase table. It should be extended by the end-operator
 * developer. The extending class should implement operationPut method and
 * provide a HBase Put metric object that specifies where and what to store for
 * the tuple in the table.<br>
 *
 * <br>
 *
 * Note that since HBase doesn't support transactions this store cannot
 * guarantee each tuple is written only once to HBase in case the operator is
 * restarted from an earlier checkpoint. It only tries to minimize the number of
 * duplicates limiting it to the tuples that were processed in the window when
 * the operator shutdown. It supports atleast once and atmost once processing
 * modes. Exactly once is not supported
 *
 * @displayName Abstract HBase Window Put Output
 * @category Output
 * @tags hbase, put, transactionable, batch
 * @param <T> The tuple type
 * @since 1.0.2
 */
public abstract class AbstractHBaseWindowPutOutputOperator<T> extends AbstractHBaseWindowOutputOperator<T>
{
  private static final transient Logger logger = LoggerFactory.getLogger(AbstractHBaseWindowPutOutputOperator.class);
  private transient ProcessingMode mode;

  @Deprecated
  public ProcessingMode getMode()
  {
    return mode;
  }

  @Deprecated
  public void setMode(ProcessingMode mode)
  {
    this.mode = mode;
  }

  public AbstractHBaseWindowPutOutputOperator()
  {
    store = new HBaseWindowStore();
  }

  @Override
  public void processTuple(T tuple, HTable table)
  {
    try {
      Put put = operationPut(tuple);
      table.put(put);
    } catch (IOException e) {
      logger.error("Could not output tuple", e);
      DTThrowable.rethrow(e);
    }
  }

  public abstract Put operationPut(T t) throws IOException;

  @Override
  public void setup(OperatorContext context)
  {
    mode = context.getValue(context.PROCESSING_MODE);
    if (mode == ProcessingMode.EXACTLY_ONCE) {
      throw new RuntimeException("This operator only supports atmost once and atleast once processing modes");
    }
    super.setup(context);
  }

}
