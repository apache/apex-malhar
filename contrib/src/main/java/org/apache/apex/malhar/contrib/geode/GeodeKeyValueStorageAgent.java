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

import java.io.Serializable;

import org.apache.apex.malhar.lib.util.AbstractKeyValueStorageAgent;
import org.apache.hadoop.conf.Configuration;

/**
 * Storage Agent implementation which uses {@link GeodeCheckpointStore} for operator
 * checkpointing
 *
 *
 *
 * @since 3.4.0
 */
public class GeodeKeyValueStorageAgent extends AbstractKeyValueStorageAgent<GeodeCheckpointStore> implements Serializable
{

  /**
   * Geode locator connection string which needs to be provided by application
   * developer/admin Format - locator1:locator1_port,locator2:locator2_port
   */
  public static final String GEODE_LOCATOR_STRING = "dt.checkpoint.agent.geode.connection";

  public GeodeKeyValueStorageAgent()
  {
    setStore(new GeodeCheckpointStore());
  }

  public GeodeKeyValueStorageAgent(Configuration conf)
  {
    setStore(new GeodeCheckpointStore(conf.get(GEODE_LOCATOR_STRING)));
  }

  /**
   * Saves yarn application id which can be used by Key value store to create
   * application specific Geode region
   */
  @Override
  public void setApplicationId(String applicationId)
  {
    store.setTableName(applicationId);
    super.setApplicationId(applicationId);
  }

  private static final long serialVersionUID = -8838327680202565778L;
}
