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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.db.Connectable;

import com.datatorrent.common.util.DTThrowable;

/**
 * CouchBaseStore which provides connect methods to Couchbase data store.
 */
public class CouchBaseStore implements Connectable
{

  protected static final Logger logger = LoggerFactory.getLogger(CouchBaseStore.class);

  @Nonnull
  protected String bucket;
  @Nonnull
  protected String password;
  @Nonnull
  protected String uriString;

  protected transient CouchbaseClient client;
  protected Integer batchSize = 100;
  protected Integer maxTuples = 1000;
  protected int blockTime = 10000;
  protected int timeout = 10000;
  protected int shutdownTimeout = 60;

  public int getTimeout()
  {
    return timeout;
  }

  public void setTimeout(int timeout)
  {
    this.timeout = timeout;
  }

  public int getBlockTime()
  {
    return blockTime;
  }

  public String getUriString()
  {
    return uriString;
  }

  public void setBlockTime(int blockTime)
  {
    this.blockTime = blockTime;
  }

  public Integer getMaxTuples()
  {
    return maxTuples;
  }

  public void setMaxTuples(Integer maxTuples)
  {
    this.maxTuples = maxTuples;
  }

  public Integer getBatchSize()
  {
    return batchSize;
  }

  public void setBatchSize(Integer batchSize)
  {
    this.batchSize = batchSize;
  }

  public int getShutdownTimeout()
  {
    return shutdownTimeout;
  }

  public void setShutdownTimeout(int shutdownTimeout)
  {
    this.shutdownTimeout = shutdownTimeout;
  }

  transient List<URI> baseURIs = new ArrayList<URI>();

  public CouchBaseStore()
  {
    client = null;
  }

  public CouchbaseClient getInstance()
  {
    return client;
  }

  public void addNodes(URI url)
  {
    baseURIs.add(url);
  }

  public void setBucket(String bucketName)
  {
    this.bucket = bucketName;
  }

  /**
   * setter for password
   *
   * @param password
   */
  public void setPassword(String password)
  {
    this.password = password;
  }

  public void setUriString(String uriString)
  {
    logger.info("In setter method of URI");
    this.uriString = uriString;
  }

  @Override
  public void connect() throws IOException
  {
    String[] tokens = uriString.split(",");
    URI uri = null;
    for (String url : tokens) {
      try {
        uri = new URI("http", url, "/pools", null, null);
      }
      catch (URISyntaxException ex) {
        DTThrowable.rethrow(ex);
      }
      baseURIs.add(uri);
    }
    try {
      CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
      cfb.setOpTimeout(timeout);  // wait up to 10 seconds for an operation to succeed
      cfb.setOpQueueMaxBlockTime(blockTime); // wait up to 10 second when trying to enqueue an operation
      client = new CouchbaseClient(cfb.buildCouchbaseConnection(baseURIs, bucket, password));
    }
    catch (IOException e) {
      logger.error("Error connecting to Couchbase: " + e.getMessage());
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public boolean isConnected()
  {
    // Not applicable for Couchbase
    return false;
  }

  @Override
  public void disconnect() throws IOException
  {
    client.shutdown(shutdownTimeout, TimeUnit.SECONDS);
  }

}
