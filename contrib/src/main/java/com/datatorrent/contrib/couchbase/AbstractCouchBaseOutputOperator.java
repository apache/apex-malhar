/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.couchbase;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator;
import com.datatorrent.lib.db.AbstractAggregateTransactionableStoreOutputOperator;
import com.google.common.collect.Lists;
import java.util.List;

/**
 * AbstractCouchBaseOutputOperator which extends Transactionable Store Output Operator.
 * Classes extending from this operator should implement the abstract functionality of generateKey, getObject and insertOrUpdate.
 * Insert and Update couchbase operators extend from this class.
 */
public abstract class AbstractCouchBaseOutputOperator<T> extends AbstractAggregateTransactionableStoreOutputOperator<T, CouchBaseWindowStore>
{
  private List<T> tuples;
  private transient Operator.ProcessingMode mode;
  protected int num_tuples;

  public Operator.ProcessingMode getMode()
  {
    return mode;
  }

  public void setMode(Operator.ProcessingMode mode)
  {
    this.mode = mode;
  }

  public AbstractCouchBaseOutputOperator()
  {
    tuples = Lists.newArrayList();
    store = new CouchBaseWindowStore();
  }

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

  @Override
  public void processTuple(T tuple)
  {
    tuples.add(tuple);
  }

  public List<T> getTuples()
  {
    return tuples;
  }

  @Override
  public void storeAggregate()
  {
    num_tuples = tuples.size();
    for (T tuple: tuples) {
      insertOrUpdate(tuple);

    }
    tuples.clear();
  }

  public abstract String generateKey(T tuple);

  public abstract Object getObject(T tuple);

  protected abstract void insertOrUpdate(T tuple);

}
