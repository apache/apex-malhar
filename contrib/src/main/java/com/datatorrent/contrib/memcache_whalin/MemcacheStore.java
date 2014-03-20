/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.memcache_whalin;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool;

import com.datatorrent.lib.db.KeyValueStore;

/**
 * Provides the implementation of a Memcache store.
 *
 * @since 0.9.3
 */
public class MemcacheStore implements KeyValueStore
{
  private static final Logger LOG = LoggerFactory.getLogger(MemcacheStore.class);
  protected transient MemCachedClient memcacheClient;
  protected transient SockIOPool pool;
  private List<String> serverAddresses = new ArrayList<String>();
  protected int keyExpiryTime = 0;

  /**
   * Adds a server address
   *
   * @param addr the address
   */
  public void addServer(InetSocketAddress addr)
  {
    serverAddresses.add(addr.getHostName() + ":" + addr.getPort());
  }

  /**
   * Gets the key expiry time.
   *
   * @return
   */
  public int getKeyExpiryTime()
  {
    return keyExpiryTime;
  }

  /**
   * Sets the key expiry time.
   *
   * @param keyExpiryTime
   */
  public void setKeyExpiryTime(int keyExpiryTime)
  {
    this.keyExpiryTime = keyExpiryTime;
  }

  @Override
  public void connect() throws IOException
  {
    pool = SockIOPool.getInstance();
    if (serverAddresses.isEmpty()) {
      pool.setServers(new String[]{"localhost:11211"});
    }
    else {
      pool.setServers(serverAddresses.toArray(new String[] {}));
    }
    pool.initialize();
    memcacheClient = new MemCachedClient();
  }

  @Override
  public void disconnect() throws IOException
  {
    pool.shutDown();
  }

  @Override
  public boolean connected()
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  /**
   * Gets the value given the key.
   * Note that it does NOT work with hash values or list values
   *
   * @param key
   * @return
   */
  @Override
  public Object get(Object key)
  {
    return memcacheClient.get(key.toString());
  }

  /**
   * Gets all the values given the keys.
   * Note that it does NOT work with hash values or list values
   *
   * @param keys
   * @return
   */
  @SuppressWarnings("unchecked")
  @Override
  public List<Object> getAll(List<Object> keys)
  {
    List<Object> results = new ArrayList<Object>();
    for (Object key : keys) {
      results.add(memcacheClient.get(key.toString()));
    }
    return results;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void put(Object key, Object value)
  {
    try {
      memcacheClient.set(key.toString(), value, keyExpiryTime);
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void putAll(Map<Object, Object> m)
  {
    for (Map.Entry<Object, Object> entry : m.entrySet()) {
      memcacheClient.set(entry.getKey().toString(), entry.getValue(), keyExpiryTime);
    }
  }

  @Override
  public void remove(Object key)
  {
    memcacheClient.delete(key.toString());
  }

}
