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
package org.apache.apex.malhar.contrib.geode;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.db.KeyValueStore;

import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.TypeMismatchException;

/**
 * Provides the implementation of a Geode store.
 * Geode is a distributed in-memory database
 *  that provides reliable asynchronous event notifications and guaranteed message delivery.
 * Geode is a data management platform that provides real-time
 * , consistent access to data-intensive applications.
 *
 *
 * @since 3.4.0
 */
public class GeodeStore implements KeyValueStore, Serializable
{
  /**
   *
   */
  private static final long serialVersionUID = -5076452548893319967L;
  private static final Logger logger = LoggerFactory.getLogger(GeodeStore.class);
  private transient ClientCache clientCache = null;
  private transient Region<Object, Object> region = null;
  private String locatorHost;
  private int locatorPort;
  private String regionName;

  private ClientCache initClient()
  {
    try {
      clientCache = new ClientCacheFactory().addPoolLocator(getLocatorHost(), getLocatorPort()).create();
    } catch (CacheClosedException ex) {
      throw new RuntimeException("Exception while creating cache", ex);

    }

    return clientCache;
  }

  /**
   * @return the regionName
   */
  public String getRegionName()
  {
    return regionName;
  }


  /**
   * @return the clientCache
   */
  public ClientCache getClientCache()
  {
    return clientCache;
  }

  /**
   * @return the locatorPort
   */
  public int getLocatorPort()
  {
    return locatorPort;
  }

  /**
   * @param locatorPort
   *          the locatorPort to set
   */
  public void setLocatorPort(int locatorPort)
  {
    this.locatorPort = locatorPort;
  }

  /**
   * @return the locatorHost
   */
  public String getLocatorHost()
  {
    return locatorHost;
  }

  /**
   * @param locatorHost
   *          the locatorHost to set
   */
  public void setLocatorHost(String locatorHost)
  {
    this.locatorHost = locatorHost;
  }

  /**
   * @return the region
   * @throws IOException
   */
  public Region<Object, Object> getRegion() throws IOException
  {
    // return region;
    if (clientCache == null || clientCache.isClosed()) {
      initClient();
    }

    if (region == null) {
      region = clientCache.getRegion(regionName);
      if (region == null) {
        region = clientCache.<Object, Object>createClientRegionFactory(ClientRegionShortcut.PROXY).create(regionName);
      }
    }

    return region;
  }

  @Override
  public void connect() throws IOException
  {
    try {
      clientCache = new ClientCacheFactory().addPoolLocator(getLocatorHost(), getLocatorPort()).create();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }

    region = clientCache.getRegion(getRegionName());

    if (region == null) {
      region = clientCache.<Object, Object>createClientRegionFactory(ClientRegionShortcut.PROXY).create(
          getRegionName());
    }

  }

  @Override
  public void disconnect() throws IOException
  {
    clientCache.close();

  }

  @Override
  public boolean isConnected()
  {
    return (clientCache.isClosed());

  }

  /**
   * Gets the value given the key. Note that it does NOT work with hash values
   * or list values
   *
   * @param key
   * @return The value.
   */
  @Override
  public Object get(Object key)
  {

    try {
      return (getRegion().get(key));
    } catch (IOException ex) {
      throw new RuntimeException("Exception while getting the object", ex);

    }

  }

  /**
   * Gets all the values given the keys. Note that it does NOT work with hash
   * values or list values
   *
   * @param keys
   * @return All values for the given keys.
   */
  @SuppressWarnings("unchecked")
  @Override
  public List<Object> getAll(List<Object> keys)
  {

    List<Object> values = new ArrayList<Object>();

    try {
      final Map<Object, Object> entries = getRegion().getAll(keys);
      for (int i = 0; i < keys.size(); i++) {
        values.add(entries.get(keys.get(i)));
      }
    } catch (IOException ex) {
      logger.info("error getting region ", ex);
    }

    return (values);

  }

  /**
   * @param regionName
   *          the regionName to set
   */
  public void setRegionName(String regionName)
  {
    this.regionName = regionName;
  }


  public Map<Object, Object> getAllMap(List<Object> keys)
  {

    try {
      final Map<Object, Object> entries = getRegion().getAll(keys);
      return (entries);
    } catch (IOException ex) {
      logger.info("error getting object ", ex);
      return null;
    }

  }

  @SuppressWarnings("rawtypes")
  public SelectResults query(String predicate)
  {
    try {
      return (getRegion().query(predicate));
    } catch (FunctionDomainException | TypeMismatchException | NameResolutionException | QueryInvocationTargetException
        | IOException e) {
      logger.info("error in querying object ", e);
      return null;
    }

  }

  @Override
  public void put(Object key, Object value)
  {
    try {
      getRegion().put(key, value);
    } catch (IOException e) {
      logger.info("while putting in region", e);
    }
  }

  @Override
  public void putAll(Map<Object, Object> map)
  {
    try {
      getRegion().putAll(map);
    } catch (IOException e) {
      logger.info("while putting all in region", e);
    }
  }

  @Override
  public void remove(Object key)
  {
    try {
      getRegion().destroy(key);
    } catch (TimeoutException | CacheWriterException | EntryNotFoundException | IOException e) {
      logger.info("while deleting", e);
    }
  }

}
