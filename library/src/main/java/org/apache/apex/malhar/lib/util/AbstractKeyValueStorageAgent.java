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
package org.apache.apex.malhar.lib.util;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.StorageAgent;

/**
 * Abstract implementation of {@link ApplicationAwareStorageAgent} which can be
 * configured be KeyValue store witch implementation of {@link StorageAgentKeyValueStore}
 *
 * NOTE - this should be picked from APEX-CORE once below feature is release
 * https://issues.apache.org/jira/browse/APEXCORE-283
 *
 * @param <S>
 *          Store implementation
 *
 * @since 3.4.0
 */
public abstract class AbstractKeyValueStorageAgent<S extends StorageAgentKeyValueStore>
    implements StorageAgent.ApplicationAwareStorageAgent, Serializable
{

  protected S store;
  protected String applicationId;
  public static final String CHECKPOINT_KEY_SEPARATOR = "-";

  /**
   * Gets the store
   *
   * @return the store
   */
  public S getStore()
  {
    return store;
  }

  /**
   * Sets the store
   *
   * @param store
   */
  public void setStore(S store)
  {
    this.store = store;
  }

  /**
   * Return yarn application id of running application
   *
   * @return
   */
  public String getApplicationId()
  {
    return applicationId;
  }

  /**
   * Set yarn application id
   *
   * @param applicationId
   */
  public void setApplicationId(String applicationId)
  {
    this.applicationId = applicationId;
  }

  /**
   * Generates key from operator id and window id to store unique operator
   * checkpoints
   *
   * @param operatorId
   * @param windowId
   * @return unique key for store
   */
  public static String generateKey(int operatorId, long windowId)
  {
    return String.valueOf(operatorId) + CHECKPOINT_KEY_SEPARATOR + String.valueOf(windowId);
  }

  /**
   * Stores the given operator object in configured store
   *
   * @param object
   *          Operator object to store
   * @param operatorId
   *          of operator
   * @param windowId
   *          window id of operator to checkpoint
   *
   */
  @Override
  public void save(Object object, int operatorId, long windowId) throws IOException
  {

    try {
      store(generateKey(operatorId, windowId), object);
      logger.debug("saved check point object key {} region {}", generateKey(operatorId, windowId), applicationId);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private synchronized void store(String checkpointKey, Object operator) throws IOException
  {
    if (!getStore().isConnected()) {
      getStore().connect();
    }
    getStore().put(checkpointKey, operator);
  }

  /**
   * Retrieves the operator object for given operator & window from configured
   * store
   *
   * @param operatorId
   *          of operator
   * @param windowId
   *          window id of operator to checkpoint
   */
  @Override
  public Object load(int operatorId, long windowId)
  {
    Object obj = null;
    try {
      obj = retrieve(generateKey(operatorId, windowId));
      logger.debug("retrieved object from store  key {} region {} ", generateKey(operatorId, windowId), applicationId);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }

    return obj;
  }

  private synchronized Object retrieve(String checkpointKey) throws IOException
  {
    if (!getStore().isConnected()) {
      getStore().connect();
    }

    return getStore().get(checkpointKey);
  }

  /**
   * Removes stored operator object for given operatorId & windowId from store
   *
   */
  @Override
  public void delete(int operatorId, long windowId) throws IOException
  {

    if (!getStore().isConnected()) {
      getStore().connect();
    }

    try {
      getStore().remove(generateKey(operatorId, windowId));
      logger.debug("deleted object from store key {} region {}", generateKey(operatorId, windowId));
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }

  }

  /**
   * Returns list window id for given operator id for which operator objects are
   * stored but not removed
   *
   */
  @Override
  public long[] getWindowIds(int operatorId) throws IOException
  {
    if (!getStore().isConnected()) {
      getStore().connect();
    }

    List<String> keys = getStore().getKeys(operatorId);
    if (keys.size() > 0) {
      long[] windowsIds = new long[keys.size()];
      int count = 0;
      for (String key : keys) {
        windowsIds[count] = extractwindowId(key);
        count++;
      }
      return windowsIds;
    } else {
      return new long[0];
    }
  }

  public static long extractwindowId(String checkpointKey)
  {
    String[] parts = checkpointKey.split(CHECKPOINT_KEY_SEPARATOR);
    return Long.parseLong(parts[1]);
  }

  /**
   * Saves the yarn application id which can be used by create application
   * specific table/region in KeyValue sore.
   */
  @Override
  public void setApplicationAttributes(AttributeMap map)
  {
    this.applicationId = map.get(DAGContext.APPLICATION_ID);
    getStore().setTableName(applicationId);
  }

  private static final long serialVersionUID = 7065320156997171116L;
  private static final Logger logger = LoggerFactory.getLogger(AbstractKeyValueStorageAgent.class);

}
