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

import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.HTable;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * A base implementation of an AggregateTransactionableStoreOutputOperator
 * operator that stores tuples in HBase columns and provides batch append.&nbsp;
 * Subclasses should provide implementation for appending operations. <br>
 * <p>
 * <br>
 * This class provides a HBase output operator that can be used to store tuples
 * in columns in a HBase table. It should be extended by the end-operator
 * developer. The extending class should implement operationAppend method and
 * provide a HBase Append metric object that specifies where and what to store
 * for the tuple in the table.<br>
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
 * @displayName Abstract HBase Window Append Output
 * @category Output
 * @tags hbase, append, transactionable, batch
 * @param <T> The tuple type
 * @since 1.0.2
 */
public abstract class AbstractHBaseWindowAppendOutputOperator<T> extends AbstractHBaseWindowOutputOperator<T>
{
  private static final transient Logger logger = LoggerFactory.getLogger(AbstractHBaseWindowAppendOutputOperator.class);
  private transient ProcessingMode mode;

  /**
   * Processing mode being a common platform feature should be set as an attribute.
   * Even if the property is set, it wouldn't effect how the platform would process that feature for the operator. */
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

  public AbstractHBaseWindowAppendOutputOperator()
  {
    store = new HBaseWindowStore();
  }

  @Override
  public void processTuple(T tuple, HTable table)
  {
    try {
      Append append = operationAppend(tuple);
      table.append(append);
    } catch (IOException e) {
      logger.error("Could not output tuple", e);
      DTThrowable.rethrow(e);
    }
  }

  /**
   * Return the HBase Append metric to store the tuple. The implementor should
   * return a HBase Append metric that specifies where and what to store for
   * the tuple in the table.
   *
   * @param t
   *            The tuple
   * @return The HBase Append metric
   */
  public abstract Append operationAppend(T t);

  @Override
  public void setup(OperatorContext context)
  {
    mode = context.getValue(OperatorContext.PROCESSING_MODE);
    if (mode == ProcessingMode.EXACTLY_ONCE) {
      throw new RuntimeException("This operator only supports atmost once and atleast once processing modes");
    }
    super.setup(context);
  }

}
