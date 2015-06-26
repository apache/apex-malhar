/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Append;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.netlet.util.DTThrowable;
import com.datatorrent.lib.db.AbstractStoreOutputOperator;

/**
 * A base implementation of a StoreOutputOperator operator that stores tuples in HBase columns and offers non-transactional append.&nbsp; Subclasses should provide implementation for appending operations. <br>
 * <p>
 * <br>
 * This class provides a HBase output operator that can be used to store tuples
 * in columns in a HBase table. It should be extended by the end-operator
 * developer. The extending class should implement operationAppend method and
 * provide a HBase Append metric object that specifies where and what to store
 * for the tuple in the table.<br>
 *
 * <br>
 * This class offers non-transactional append where the columns are append as
 * the tuples come in without waiting for the end window
 * @displayName Abstract HBase Append Output
 * @category Store
 * @tags output operator
 * @param <T>
 *            The tuple type
 * @since 1.0.2
 */
public abstract class AbstractHBaseAppendOutputOperator<T>
extends AbstractStoreOutputOperator<T, HBaseStore> {
  private static final transient Logger logger = LoggerFactory
      .getLogger(AbstractHBaseAppendOutputOperator.class);

  public AbstractHBaseAppendOutputOperator() {
    store = new HBaseStore();
  }

  @Override
  public void processTuple(T tuple) {
    Append append = operationAppend(tuple);
    try {
      store.getTable().append(append);
    } catch (IOException e) {
      logger.error("Could not append tuple", e);
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
}
