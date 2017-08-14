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
package org.apache.apex.malhar.contrib.accumulo;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.apex.malhar.lib.db.AbstractAggregateTransactionableStoreOutputOperator;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * Base output operator that stores tuples in Accumulo rows.&nbsp; Subclasses should provide implementation of operationMutation method. <br>
 *
 * <br>
 * This class provides a Accumulo output operator that can be used to store
 * tuples in rows in a Accumulo table. It should be extended to provide specific
 * implementation. The extending class should implement operationMutation method
 * and provide a Accumulo Mutation metric object that specifies where and what
 * to store for the tuple in the table.<br>
 *
 * <br>
 * This class provides a batch put where tuples are collected till the end
 * window and they are put on end window
 *
 * Note that since Accumulo doesn't support transactions this store cannot
 * guarantee each tuple is written only once to Accumulo in case the operator is
 * restarted from an earlier checkpoint. It only tries to minimize the number of
 * duplicates limiting it to the tuples that were processed in the window when
 * the operator shutdown.
 * @displayName Abstract Accumulo Output
 * @category Output
 * @tags accumulo, key value
 * @param <T>
 *            The tuple type
 * @since 1.0.4
 */
public abstract class AbstractAccumuloOutputOperator<T> extends AbstractAggregateTransactionableStoreOutputOperator<T, AccumuloWindowStore>
{
  private static final transient Logger logger = LoggerFactory.getLogger(AbstractAccumuloOutputOperator.class);
  private final List<T> tuples;
  private transient ProcessingMode mode;
  public ProcessingMode getMode()
  {
    return mode;
  }

  public void setMode(ProcessingMode mode)
  {
    this.mode = mode;
  }

  public AbstractAccumuloOutputOperator()
  {
    tuples = Lists.newArrayList();
    store = new AccumuloWindowStore();
  }

  @Override
  public void processTuple(T tuple)
  {
    tuples.add(tuple);
  }

  @Override
  public void storeAggregate()
  {
    try {
      for (T tuple : tuples) {
        Mutation mutation = operationMutation(tuple);
        store.getBatchwriter().addMutation(mutation);
      }
      store.getBatchwriter().flush();

    } catch (MutationsRejectedException e) {
      logger.error("unable to write mutations", e);
      DTThrowable.rethrow(e);
    }
    tuples.clear();
  }

  /**
   *
   * @param t
   * @return Mutation
   */
  public abstract Mutation operationMutation(T t);

  @Override
  public void setup(OperatorContext context)
  {
    mode = context.getValue(context.PROCESSING_MODE);
    if (mode == ProcessingMode.EXACTLY_ONCE) {
      throw new RuntimeException("This operator only supports atmost once and atleast once processing modes");
    }
    if (mode == ProcessingMode.AT_MOST_ONCE) {
      tuples.clear();
    }
    super.setup(context);
  }
}

