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
package org.apache.apex.malhar.contrib.cassandra;

import java.util.Map;

import com.datastax.driver.core.TypeCodec;

/**
 A concrete implementation of the AbstractUpsertOperator that tests composite primary key use cases
 Please run the schema as given in {@link AbstractUpsertOutputOperatorCodecsTest}
 */
public class CompositePrimaryKeyUpdateOperator extends AbstractUpsertOutputOperator
{
  @Override
  public ConnectionStateManager.ConnectionBuilder withConnectionBuilder()
  {
    return ConnectionStateManager.withNewBuilder()
                .withSeedNodes("localhost")
                .withClusterNameAs("Test Cluster")
                .withDCNameAs("datacenter1")
                .withTableNameAs("userstatus")
                .withKeySpaceNameAs("unittests");
  }

  @Override
  public Map<String, TypeCodec> getCodecsForUserDefinedTypes()
  {
    return null;
  }

  @Override
  public Class getPayloadPojoClass()
  {
    return CompositePrimaryKeyRow.class;
  }

  @Override
  boolean reconcileRecord(Object T, long windowId)
  {
    return true;
  }
}
