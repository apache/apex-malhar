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
package org.apache.apex.benchmark.aerospike;

import java.util.List;

import org.apache.apex.malhar.contrib.aerospike.AbstractAerospikeTransactionalPutOperator;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;

/**
 * <p>AerospikeOutputOperator class.</p>
 *
 * @since 1.0.4
 */
public class AerospikeOutputOperator extends AbstractAerospikeTransactionalPutOperator<Integer>
{

  private final String KEYSPACE = "test";
  private final String SET_NAME = "Aerospike_Output";
  private int id = 0;

  @Override
  protected Key getUpdatedBins(Integer tuple, List<Bin> bins)
    throws AerospikeException
  {

    Key key = new Key(KEYSPACE, SET_NAME, id++);
    bins.add(new Bin("ID", tuple));
    return key;
  }

}
