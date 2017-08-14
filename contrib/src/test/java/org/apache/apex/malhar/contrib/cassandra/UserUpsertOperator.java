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

import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;

/**
  This operator is a concrete implementation of the {@link AbstractUpsertOutputOperator}. This is used
  in unit tests ( refer {@link AbstractUpsertOutputOperatorCodecsTest} to showcase the use cases of:
    => Connectivity options
    => Codecs for uder defined types
    => How to specify null overrides to avoid patterns of read and then set values in a row
    => Collection mutations Add / Remove
    => List placements Append/Prepend if there is a list column
    => Map POJO field names to Cassandra columns if they do not match exactly
    => Specifying TTL
    => Overriding default consistency level set with Connection builder at a tuple level if required
 */
public class UserUpsertOperator extends AbstractUpsertOutputOperator
{
  @Override
  public ConnectionStateManager.ConnectionBuilder withConnectionBuilder()
  {
    return ConnectionStateManager.withNewBuilder()
                .withSeedNodes("localhost")
                .withClusterNameAs("Test Cluster")
                .withDCNameAs("datacenter1")
                .withTableNameAs("users")
                .withKeySpaceNameAs("unittests")
                .withdefaultConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
  }

  @Override
  public Map<String, TypeCodec> getCodecsForUserDefinedTypes()
  {
    Map<String, TypeCodec> allCodecs = new HashMap<>();
    CodecRegistry codecRegistry = cluster.getConfiguration().getCodecRegistry();

    UserType addressType = cluster.getMetadata().getKeyspace(getConnectionStateManager().getKeyspaceName())
        .getUserType("address");
    TypeCodec<UDTValue> addressTypeCodec = codecRegistry.codecFor(addressType);
    AddressCodec addressCodec = new AddressCodec(addressTypeCodec, Address.class);
    allCodecs.put("currentaddress", addressCodec);

    UserType userFullNameType = cluster.getMetadata().getKeyspace(getConnectionStateManager().getKeyspaceName())
        .getUserType("fullname");
    TypeCodec<UDTValue> userFullNameTypeCodec = codecRegistry.codecFor(userFullNameType);
    FullNameCodec fullNameCodec = new FullNameCodec(userFullNameTypeCodec, FullName.class);
    allCodecs.put("username", fullNameCodec);

    return allCodecs;
  }

  @Override
  public Class getPayloadPojoClass()
  {
    return User.class;
  }


  @Override
  protected Map<String, String> getPojoFieldNameToCassandraColumnNameOverride()
  {
    Map<String,String> overridingColMap = new HashMap<>();
    overridingColMap.put("topScores","top_scores");
    return overridingColMap;
  }

  @Override
  boolean reconcileRecord(Object T, long windowId)
  {
    return true;
  }
}

