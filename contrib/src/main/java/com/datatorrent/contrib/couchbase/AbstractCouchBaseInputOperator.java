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

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.vbucket.config.Config;
import java.io.IOException;
import java.util.List;

import com.datatorrent.lib.db.AbstractStoreInputOperator;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;

import com.datatorrent.common.util.DTThrowable;
import static com.datatorrent.contrib.couchbase.CouchBaseStore.logger;
import com.esotericsoftware.kryo.Kryo;
import org.apache.commons.io.output.ByteArrayOutputStream;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AbstractCouchBaseInputOperator which extends AbstractStoreInputOperator.
 * Classes extending from this operator should implement the abstract functionality of getTuple and getKeys.
 */
public abstract class AbstractCouchBaseInputOperator<T> extends AbstractStoreInputOperator<T, CouchBaseStore> implements Partitioner<AbstractCouchBaseInputOperator<T>>
{

  //need to save this,hence non transient.
  protected static final Logger logger = LoggerFactory.getLogger(CouchBaseStore.class);
  protected transient CouchbaseClient clientPartition = null;

  private int serverIndex;
  private String urlString;

  public String getUrlString()
  {
    return urlString;
  }

  public void setUrlString(String urlString)
  {
    this.urlString = urlString;
  }

  protected transient Config conf;

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
    if (conf == null) {
      conf = store.getConf();
    }
    if (clientPartition == null) {
      ArrayList<URI> nodes = new ArrayList<URI>();

      urlString = urlString.replace("default", "pools");
      try {
        nodes.add(new URI(urlString));
      }
      catch (URISyntaxException ex) {
        DTThrowable.rethrow(ex);
      }

      try {
        clientPartition = new CouchbaseClient(nodes, "default", "");
      }
      catch (IOException e) {
        logger.error("Error connecting to Couchbase: " + e.getMessage());
        DTThrowable.rethrow(e);
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
    for (String key: keys) {
      //if(store.conf.getMaster(store.conf.getVbucketByKey(key))){
      int master = conf.getMaster(conf.getVbucketByKey(key));
      if (master == getServerIndex()) {
        logger.info("master is {}", master);
        logger.info("urlstring is {}", urlString);
        Object result = clientPartition.get(key);
        logger.info("result is {} urlstring is {}", result, urlString);
        if (result != null) {
          T tuple = getTuple(result);
          outputPort.emit(tuple);
        }

      }
    }
    //  }

  }

  public abstract T getTuple(Object object);

  public abstract List<String> getKeys();

  @Override
  public void partitioned(Map<Integer, Partition<AbstractCouchBaseInputOperator<T>>> partitions)
  {

  }

  @Override
  public Collection<Partition<AbstractCouchBaseInputOperator<T>>> definePartitions(Collection<Partition<AbstractCouchBaseInputOperator<T>>> partitions, int incrementalCapacity)
  {
    conf = store.getConf();
    int numPartitions = conf.getCouchServers().size();
    List<URL> list = conf.getCouchServers();
    Collection<Partition<AbstractCouchBaseInputOperator<T>>> newPartitions = Lists.newArrayListWithExpectedSize(numPartitions);
    Kryo kryo = new Kryo();
    for (int i = 0; i < numPartitions; i++) {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Output output = new Output(bos);
      kryo.writeObject(output, this);
      output.close();
      Input lInput = new Input(bos.toByteArray());
      @SuppressWarnings("unchecked")
      AbstractCouchBaseInputOperator<T> oper = kryo.readObject(lInput, this.getClass());
      oper.setServerIndex(i);
      oper.setUrlString(list.get(i).toString());
      logger.info("oper {} urlstring is {}", i, oper.getUrlString());
      // oper.setStore(this.store);
      newPartitions.add(new DefaultPartition<AbstractCouchBaseInputOperator<T>>(oper));
    }

    return newPartitions;

  }

}
