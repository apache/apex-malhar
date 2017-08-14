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
package org.apache.apex.malhar.contrib.couchbase;

import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.db.AbstractAggregateTransactionableStoreOutputOperator;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.netlet.util.DTThrowable;

import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.internal.OperationFuture;


/**
 * AbstractCouchBaseOutputOperator which extends Transactionable Store Output Operator.
 * Classes extending from this operator should implement the abstract functionality of generateKey, getValue and insertOrUpdate.
 * Insert and Update couchbase operators extend from this class.
 * If the processing mode is EXACTLY ONCE, then in case of update the functionality will always be correct since it will overwrite the same keys
 * but for inserts there might be duplicates when operator fails.
 *
 * @since 2.0.0
 */
public abstract class AbstractCouchBaseOutputOperator<T> extends AbstractAggregateTransactionableStoreOutputOperator<T, CouchBaseWindowStore>
{
  private static final transient Logger logger = LoggerFactory.getLogger(AbstractCouchBaseOutputOperator.class);
  protected transient HashMap<OperationFuture<Boolean>, Long> mapFuture;
  protected int numTuples;
  protected CouchBaseSerializer serializer;
  protected TreeMap<Long, T> mapTuples;
  protected long id = 0;
  private final transient CompletionListener listener;
  private transient boolean failure;
  private final transient Object syncObj;

  public AbstractCouchBaseOutputOperator()
  {
    mapFuture = new HashMap<OperationFuture<Boolean>, Long>();
    mapTuples = new TreeMap<Long, T>();
    store = new CouchBaseWindowStore();
    listener = new CompletionListener();
    numTuples = 0;
    syncObj = new Object();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setup(OperatorContext context)
  {
    ProcessingMode mode = context.getValue(OperatorContext.PROCESSING_MODE);
    if (mode == ProcessingMode.AT_MOST_ONCE) {
      //map must be cleared to avoid writing same data twice
      if (numTuples == 0) {
        mapTuples.clear();
      }
    }
    numTuples = 0;
    //atleast once, check leftovers in map and send them to couchbase.
    if (!mapTuples.isEmpty()) {
      Iterator<?> itr = mapTuples.values().iterator();
      while (itr.hasNext()) {
        processTuple((T)itr.next());
      }
    }
    super.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    numTuples = 0;
    super.beginWindow(windowId);
  }

  @Override
  public void processTuple(T tuple)
  {
    if (failure) {
      throw new RuntimeException("Operation Failed");
    }
    waitForQueueSize(store.queueSize - 1);
    setKeyValueInCouchBase(tuple);
  }

  public void setKeyValueInCouchBase(T tuple)
  {
    id++;
    String key = getKey(tuple);
    Object value = getValue(tuple);
    if (!(value instanceof Boolean) && !(value instanceof Integer) && !(value instanceof String) && !(value instanceof Float) && !(value instanceof Double) && !(value instanceof Character) && !(value instanceof Long) && !(value instanceof Short) && !(value instanceof Byte)) {
      if (serializer != null) {
        value = serializer.serialize(value);
      }
    }
    OperationFuture<Boolean> future = processKeyValue(key, value);
    synchronized (syncObj) {
      future.addListener(listener);
      mapFuture.put(future, id);
      if (!mapTuples.containsKey(id)) {
        mapTuples.put(id, tuple);
      }
      numTuples++;
    }

  }

  @Override
  public void storeAggregate()
  {
    waitForQueueSize(0);
    id = 0;
  }

  public void waitForQueueSize(int sizeOfQueue)
  {
    long startTms = System.currentTimeMillis();
    long elapsedTime;
    while (numTuples > sizeOfQueue) {
      synchronized (syncObj) {
        if (numTuples > sizeOfQueue) {
          try {
            elapsedTime = System.currentTimeMillis() - startTms;
            if (elapsedTime >= store.timeout) {
              throw new RuntimeException("Timed out waiting for space in queue");
            } else {
              syncObj.wait(store.timeout - elapsedTime);
            }
          } catch (InterruptedException ex) {
            DTThrowable.rethrow(ex);
          }
        }
      }
      elapsedTime = System.currentTimeMillis() - startTms;
      if (elapsedTime >= store.timeout) {
        throw new RuntimeException("Timed out waiting for space in queue");
      }

    }
  }

  protected class CompletionListener implements OperationCompletionListener
  {
    @Override
    public void onComplete(OperationFuture<?> f) throws Exception
    {
      if (!((Boolean)f.get())) {
        logger.error("Operation failed {}", f);
        failure = true;
        return;
      }
      synchronized (syncObj) {
        long idProcessed = mapFuture.get(f);
        mapTuples.remove(idProcessed);
        mapFuture.remove(f);
        numTuples--;
        syncObj.notify();
      }
    }

  }

  public CouchBaseSerializer getSerializer()
  {
    return serializer;
  }

  public void setSerializer(CouchBaseSerializer serializer)
  {
    this.serializer = serializer;
  }

  public abstract String getKey(T tuple);

  public abstract Object getValue(T tuple);

  protected abstract OperationFuture<Boolean> processKeyValue(String key, Object value);

}
