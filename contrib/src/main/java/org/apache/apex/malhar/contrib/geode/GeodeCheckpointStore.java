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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.apex.malhar.lib.util.StorageAgentKeyValueStore;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.google.common.collect.Maps;

/**
 * Geode Store implementation of {@link StorageAgentKeyValueStore} Uses {@link Kryo}
 * serialization to store retrieve objects
 *
 *
 *
 * @since 3.4.0
 */
public class GeodeCheckpointStore
    implements StorageAgentKeyValueStore, Serializable
{

  public static final String GET_KEYS_QUERY =
      "SELECT entry.key FROM /$[region}.entries entry WHERE entry.key LIKE '${operator.id}%'";

  private String geodeLocators;
  private String geodeRegionName;

  public String getGeodeRegionName()
  {
    return geodeRegionName;
  }

  public void setGeodeRegionName(String geodeRegionName)
  {
    this.geodeRegionName = geodeRegionName;
  }

  protected transient Kryo kryo;

  public GeodeCheckpointStore()
  {
    geodeLocators = null;
    kryo = null;
  }

  /**
   * Initializes Geode store by using locator connection string
   *
   * @param locatorString
   */
  public GeodeCheckpointStore(String locatorString)
  {
    this.geodeLocators = locatorString;
    kryo = new Kryo();
  }

  private Kryo getKyro()
  {
    if (kryo == null) {
      kryo = new Kryo();
    }
    return kryo;
  }

  /**
   * Get the Geode locator connection string
   *
   * @return locator connection string
   */
  public String getGeodeLocators()
  {
    return geodeLocators;
  }

  /**
   * Sets the Geode locator string
   *
   * @param geodeLocators
   */
  public void setGeodeLocators(String geodeLocators)
  {
    this.geodeLocators = geodeLocators;
  }

  private transient ClientCache clientCache = null;
  private transient Region<String, byte[]> region = null;

  /**
   * Connect the Geode store by initializing Geode Client Cache
   */
  @Override
  public void connect() throws IOException
  {
    ClientCacheFactory factory = new ClientCacheFactory();
    Map<String, String> locators = parseLocatorString(geodeLocators);

    if (locators.size() == 0) {
      throw new IllegalArgumentException("Invalid locator connection string " + geodeLocators);
    } else {
      for (Entry<String, String> entry : locators.entrySet()) {
        factory.addPoolLocator(entry.getKey(), Integer.valueOf(entry.getValue()));
      }
    }
    clientCache = factory.create();
  }

  private Region<String, byte[]> getGeodeRegion() throws IOException
  {
    if (clientCache == null) {
      this.connect();
    }
    if (region == null) {
      region = clientCache.getRegion(geodeRegionName);
      if (region == null) {
        createRegion();
        region = clientCache.<String, byte[]>createClientRegionFactory(ClientRegionShortcut.PROXY)
            .create(geodeRegionName);
      }
    }

    return region;
  }

  /**
   * Creates a region
   *
   */
  public synchronized void createRegion()
  {
    RegionCreateFunction atcf = new RegionCreateFunction();
    java.util.List<Object> inputList = new java.util.ArrayList<Object>();

    inputList.add(geodeRegionName);
    inputList.add(true);

    Execution members = FunctionService.onServers(clientCache.getDefaultPool()).withArgs(inputList);
    members.execute(atcf.getId()).getResult();
  }

  /**
   * Disconnect the connection to Geode store by closing Client Cache connection
   */
  @Override
  public void disconnect() throws IOException
  {
    clientCache.close();
  }

  /**
   * Check if store is connected to configured Geode cluster or not
   *
   * @return True is connected to Geode cluster and client cache is active
   */
  @Override
  public boolean isConnected()
  {
    if (clientCache == null) {
      return false;
    }
    return !clientCache.isClosed();
  }

  /**
   * Return the value for specified key from Geode region
   *
   * @return the value object
   */
  @Override
  public Object get(Object key)
  {

    try {
      byte[] obj = getGeodeRegion().get((String)key);
      if (obj == null) {
        return null;
      }

      ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(obj);
      getKyro().setClassLoader(Thread.currentThread().getContextClassLoader());
      Input input = new Input(byteArrayInputStream);
      return getKyro().readClassAndObject(input);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Put given given key & value in Geode region
   */
  @Override
  public void put(Object key, Object value)
  {
    try {
      Output output = new Output(4096, Integer.MAX_VALUE);
      getKyro().writeClassAndObject(output, value);
      getGeodeRegion().put((String)key, output.getBuffer());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Removed the record associated for specified key from Geode region
   */
  @Override
  public void remove(Object key)
  {
    try {
      getGeodeRegion().destroy((String)key);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get list for keys starting from provided key name
   *
   * @return List of keys
   */
  @Override
  public List<String> getKeys(Object key)
  {
    List<String> keys = null;
    try {
      keys = queryIds((int)(key));
      return keys;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public List<String> queryIds(int operatorId) throws IOException
  {
    List<String> ids = new ArrayList<>();
    try {
      QueryService queryService = clientCache.getQueryService();
      Query query = queryService.newQuery(
          GET_KEYS_QUERY.replace("$[region}", geodeRegionName).replace("${operator.id}", String.valueOf(operatorId)));
      logger.debug("executing query {} ", query.getQueryString());

      SelectResults results = (SelectResults)query.execute();
      for (Iterator iterator = results.iterator(); iterator.hasNext();) {
        ids.add(String.valueOf(iterator.next()));
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return ids;
  }

  /**
   * Sets the region name for this Store instance to connect to
   */
  @Override
  public void setTableName(String tableName)
  {
    this.geodeRegionName = tableName;
  }

  private Map<String, String> parseLocatorString(String locatorConnString)
  {
    Map<String, String> locators = Maps.newHashMap();
    for (String locator : locatorConnString.split(",")) {
      String[] parts = locator.split(":");
      if (parts.length > 1 && !parts[0].isEmpty() && parts[0] != "" && !parts[1].isEmpty() && parts[1] != "") {
        locators.put(parts[0], parts[1]);
      } else {
        throw new IllegalArgumentException("Wrong locator connection string : " + locatorConnString + "\n"
            + "Expected format locator1:locator1_port,locator2:locator2_port");
      }
    }
    return locators;
  }

  private static final long serialVersionUID = 8897644407674960149L;
  private static final Logger logger = LoggerFactory.getLogger(GeodeCheckpointStore.class);


  @Override
  public List<Object> getAll(List<Object> keys)
  {
    return null;
  }

  @Override
  public void putAll(Map<Object, Object> m)
  {
  }

}
