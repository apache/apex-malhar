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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.db.AbstractStoreInputOperator;
import org.apache.apex.malhar.lib.util.KryoCloneUtils;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.vbucket.config.Config;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * AbstractCouchBaseInputOperator which extends AbstractStoreInputOperator.
 * Classes extending from this operator should implement the abstract functionality of getTuple and getKeys.
 *
 * @since 2.0.0
 */
public abstract class AbstractCouchBaseInputOperator<T> extends AbstractStoreInputOperator<T, CouchBaseStore> implements Partitioner<AbstractCouchBaseInputOperator<T>>
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractCouchBaseInputOperator.class);
  protected transient CouchbaseClient clientPartition = null;

  private int serverIndex;

  protected transient Config conf;

  // URL specific to a server.
  protected String serverURIString;

  public String getServerURIString()
  {
    return serverURIString;
  }

  @VisibleForTesting
  public void setServerURIString(String serverURIString)
  {
    this.serverURIString = serverURIString;
  }

  public int getServerIndex()
  {
    return serverIndex;
  }

  public void setServerIndex(int serverIndex)
  {
    this.serverIndex = serverIndex;
  }

  public AbstractCouchBaseInputOperator()
  {
    store = new CouchBaseStore();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    if (clientPartition == null) {
      if (conf == null) {
        conf = store.getConf();
      }
      try {
        clientPartition = store.connectServer(serverURIString);
      } catch (IOException ex) {
        DTThrowable.rethrow(ex);
      }
    }
  }

  @Override
  public void teardown()
  {
    if (clientPartition != null) {
      clientPartition.shutdown(store.shutdownTimeout, TimeUnit.SECONDS);
    }
    super.teardown();
  }

  @Override
  public void emitTuples()
  {
    List<String> keys = getKeys();
    Object result = null;
    for (String key: keys) {
      int master = conf.getMaster(conf.getVbucketByKey(key));
      if (master == getServerIndex()) {
        result = clientPartition.get(key);
      }
    }

    if (result != null) {
      T tuple = getTuple(result);
      outputPort.emit(tuple);
    }
  }

  public abstract T getTuple(Object object);

  public abstract List<String> getKeys();

  @Override
  public void partitioned(Map<Integer, Partition<AbstractCouchBaseInputOperator<T>>> partitions)
  {

  }

  @Override
  public Collection<Partition<AbstractCouchBaseInputOperator<T>>> definePartitions(Collection<Partition<AbstractCouchBaseInputOperator<T>>> partitions, PartitioningContext incrementalCapacity)
  {
    conf = store.getConf();
    int numPartitions = conf.getServers().size();
    List<String> list = conf.getServers();
    Collection<Partition<AbstractCouchBaseInputOperator<T>>> newPartitions = Lists.newArrayListWithExpectedSize(numPartitions);
    KryoCloneUtils<AbstractCouchBaseInputOperator<T>> cloneUtils = KryoCloneUtils.createCloneUtils(this);
    for (int i = 0; i < numPartitions; i++) {
      AbstractCouchBaseInputOperator<T> oper = cloneUtils.getClone();
      oper.setServerIndex(i);
      oper.setServerURIString(list.get(i));
      logger.debug("oper {} urlstring is {}", i, oper.getServerURIString());
      newPartitions.add(new DefaultPartition<>(oper));
    }

    return newPartitions;

  }

}
