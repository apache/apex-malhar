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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.apex.malhar.lib.db.Connectable;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.couchbase.client.vbucket.ConfigurationProvider;
import com.couchbase.client.vbucket.ConfigurationProviderHTTP;
import com.couchbase.client.vbucket.config.Bucket;
import com.couchbase.client.vbucket.config.Config;

import com.datatorrent.netlet.util.DTThrowable;

/**
 * CouchBaseStore which provides connect methods to Couchbase data store.
 *
 * @since 2.0.0
 */
public class CouchBaseStore implements Connectable
{

  private static final Logger logger = LoggerFactory.getLogger(CouchBaseStore.class);
  @Nonnull
  protected String bucket;

  public String getBucket()
  {
    return bucket;
  }

  public void setBucket(String bucket)
  {
    this.bucket = bucket;
  }

  @Nonnull
  protected String password;
  @Nonnull
  protected String userConfig;

  public String getUserConfig()
  {
    return userConfig;
  }

  public void setUserConfig(String userConfig)
  {
    this.userConfig = userConfig;
  }

  public String getPasswordConfig()
  {
    return passwordConfig;
  }

  public void setPasswordConfig(String passwordConfig)
  {
    this.passwordConfig = passwordConfig;
  }

  @Nonnull
  protected String passwordConfig;

  @Nonnull
  protected String uriString;

  protected transient CouchbaseClient client;
  @Min(1)
  protected Integer queueSize = 100;

  public Integer getQueueSize()
  {
    return queueSize;
  }

  public void setQueueSize(Integer queueSize)
  {
    this.queueSize = queueSize;
  }

  protected Integer maxTuples = 1000;
  protected int blockTime = 1000;
  protected long timeout = 10000;
  protected int shutdownTimeout = 60;

  public long getTimeout()
  {
    return timeout;
  }

  public void setTimeout(long timeout)
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
    bucket = "default";
  }

  public CouchbaseClient getInstance()
  {
    return client;
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
    this.uriString = uriString;
  }

  public Config getConf()
  {
    try {
      connect();
    } catch (IOException ex) {
      DTThrowable.rethrow(ex);
    }
    ConfigurationProvider configurationProvider = new ConfigurationProviderHTTP(baseURIs, userConfig, passwordConfig);
    Bucket configBucket = configurationProvider.getBucketConfiguration(bucket);
    Config conf = configBucket.getConfig();
    try {
      disconnect();
    } catch (IOException ex) {
      DTThrowable.rethrow(ex);
    }
    return conf;
  }

  @Override
  public void connect() throws IOException
  {
    String[] tokens = uriString.split(",");
    URI uri = null;
    for (String url : tokens) {
      try {
        uri = new URI("http", url, "/pools", null, null);
      } catch (URISyntaxException ex) {
        DTThrowable.rethrow(ex);
      }
      baseURIs.add(uri);
    }
    try {
      CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
      cfb.setOpTimeout(timeout);  // wait up to 10 seconds for an operation to succeed
      cfb.setOpQueueMaxBlockTime(blockTime); // wait up to 10 second when trying to enqueue an operation
      client = new CouchbaseClient(cfb.buildCouchbaseConnection(baseURIs, bucket, password));
      //client = new CouchbaseClient(baseURIs, "default", "");
    } catch (IOException e) {
      logger.error("Error connecting to Couchbase:", e);
      DTThrowable.rethrow(e);
    }
  }

  public CouchbaseClient connectServer(String urlString) throws IOException
  {
    ArrayList<URI> nodes = new ArrayList<URI>();
    CouchbaseClient clientPartition = null;
    try {
      nodes.add(new URI("http",urlString,"/pools", null, null));
    } catch (URISyntaxException ex) {
      DTThrowable.rethrow(ex);
    }
    try {
      clientPartition = new CouchbaseClient(nodes, bucket, password);
    } catch (IOException e) {
      logger.error("Error connecting to Couchbase:", e);
      DTThrowable.rethrow(e);
    }
    return clientPartition;

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
    if (client != null) {
      client.shutdown(shutdownTimeout, TimeUnit.SECONDS);
    }
  }

}
