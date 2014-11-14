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

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;
import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.internal.OperationFuture;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.db.AbstractAggregateTransactionableStoreOutputOperator;

import com.datatorrent.api.Context.OperatorContext;

import com.datatorrent.common.util.DTThrowable;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * AbstractCouchBaseOutputOperator which extends Transactionable Store Output Operator.
 * Classes extending from this operator should implement the abstract functionality of generateKey, getValue and insertOrUpdate.
 * Insert and Update couchbase operators extend from this class.
 * If the processing mode is EXACTLY ONCE, then in case of update the functionality will always be correct since it will overwrite the same keys
 * but for inserts there might be duplicates when operator fails.
 *
 */
public abstract class AbstractCouchBaseOutputOperator<T> extends AbstractAggregateTransactionableStoreOutputOperator<T, CouchBaseWindowStore>
{
  private static final transient Logger logger = LoggerFactory.getLogger(AbstractCouchBaseOutputOperator.class);
  protected transient ConcurrentHashMap<OperationFuture, Long> mapFuture;
  protected transient AtomicInteger numTuples;
  protected CouchBaseSerializer serializer;
  protected ConcurrentSkipListMap<Long, T> mapTuples;
  protected long id = 0;
  protected transient OperationFuture<Boolean> future;
  private transient CompletionListener listener;

  public AbstractCouchBaseOutputOperator()
  {
    mapFuture = new ConcurrentHashMap<OperationFuture, Long>();
    mapTuples = new ConcurrentSkipListMap<Long, T>();
    store = new CouchBaseWindowStore();
    listener = new CompletionListener();

  }

  @Override
  public void setup(OperatorContext context)
  {
    ProcessingMode mode = context.getValue(context.PROCESSING_MODE);
    if (mode == ProcessingMode.AT_MOST_ONCE || mode == ProcessingMode.AT_LEAST_ONCE) {
      //map must be cleared to avoid writing same data twice
      mapTuples.clear();
    }

    //atleast once, check leftovers in map and send them to couchbase.
    if (!mapTuples.isEmpty()) {
      Iterator itr = mapTuples.values().iterator();
      while (itr.hasNext()) {
        processTuple((T)itr.next());
      }
    }
    super.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    numTuples = new AtomicInteger(0);
    super.beginWindow(windowId);
  }

  @Override
  public void processTuple(T tuple)
  {
    waitForBatch(false);
    setKeyValueInCouchBase(tuple);
  }

  public void setKeyValueInCouchBase(T tuple)
  {
    id++;
    String key = generateKey(tuple);
    Object value = getValue(tuple);
    if (serializer != null) {
      value = serializer.serialize(value);
    }
    future = processKeyValue(key, value);
    future.addListener(listener);
    mapFuture.put(future, id);
    if (!mapTuples.containsKey(id)) {
      mapTuples.put(id, tuple);
    }
    numTuples.incrementAndGet();
  }

  @Override
  public void storeAggregate()
  {
    waitForBatch(true);
    id = 0;
  }

  public void waitForBatch(boolean isEndWindow)
  {
    while ((numTuples.get() > store.batchSize) || (isEndWindow && (numTuples.get() > 0))) {
      long startTms = System.currentTimeMillis();
      synchronized (numTuples) {
        try {
          long elapsedTime = System.currentTimeMillis() - startTms;
          numTuples.wait(store.timeout - elapsedTime);
        }
        catch (InterruptedException ex) {
          logger.info(AbstractCouchBaseOutputOperator.class.getName() + ex);
        }
      }
    }

  }

  protected class CompletionListener implements OperationCompletionListener
  {
    @Override
    public void onComplete(OperationFuture<?> f) throws Exception
    {
      if (!((Boolean)f.get())) {
        throw new RuntimeException("Operation failed " + f);
      }
      long idProcessed = mapFuture.get(f);
      //listID.remove(idProcessed);
      if (mapTuples.containsKey(idProcessed)) {
        mapTuples.remove(idProcessed);
      }
      if (mapFuture.containsKey(f)) {
        mapFuture.remove(f);
      }
      numTuples.decrementAndGet();
      synchronized (numTuples) {
        numTuples.notify();
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

  public abstract String generateKey(T tuple);

  public abstract Object getValue(T tuple);

  protected abstract OperationFuture processKeyValue(String key, Object value);

}
