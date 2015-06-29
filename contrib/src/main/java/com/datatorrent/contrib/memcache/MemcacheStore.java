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
package com.datatorrent.contrib.memcache;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import net.spy.memcached.MemcachedClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.db.KeyValueStore;

/**
 * Provides the implementation of a Memcache store.
 *
 * @since 0.9.3
 */
public class MemcacheStore implements KeyValueStore
{
  private static final Logger LOG = LoggerFactory.getLogger(MemcacheStore.class);
  protected transient MemcachedClient memcacheClient;
  private List<InetSocketAddress> serverAddresses = new ArrayList<InetSocketAddress>();
  protected int keyExpiryTime = 0;

  /**
   * Adds a server address
   *
   * @param addr the address
   */
  public void addServer(InetSocketAddress addr)
  {
    serverAddresses.add(addr);
  }
  

  public List<InetSocketAddress> getServerAddresses()
  {
    return serverAddresses;
  }


  public void setServerAddresses(List<InetSocketAddress> serverAddresses)
  {
    this.serverAddresses = serverAddresses;
  }


  /**
   * Gets the key expiry time.
   *
   * @return The key expiry time.
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
    if (serverAddresses.isEmpty()) {
      memcacheClient = new MemcachedClient(new InetSocketAddress("localhost", 11211));
    }
    else {
      memcacheClient = new MemcachedClient(serverAddresses);
    }
  }

  @Override
  public void disconnect() throws IOException
  {
    memcacheClient.shutdown();
  }

  @Override
  public boolean isConnected()
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  /**
   * Gets the value given the key.
   * Note that it does NOT work with hash values or list values
   *
   * @param key
   * @return The value.
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
   * @return All values for the given keys.
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
      memcacheClient.set(key.toString(), keyExpiryTime, value).get();
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void putAll(Map<Object, Object> m)
  {
    List<Future<?>> futures = new ArrayList<Future<?>>();
    for (Map.Entry<Object, Object> entry : m.entrySet()) {
      futures.add(memcacheClient.set(entry.getKey().toString(), keyExpiryTime, entry.getValue()));
    }
    for (Future<?> future : futures) {
      try {
        future.get();
      }
      catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  @Override
  public void remove(Object key)
  {
    memcacheClient.delete(key.toString());
  }

}
