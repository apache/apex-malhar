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
package org.apache.apex.malhar.contrib.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TupleGenerateCacheOperator<T> extends POJOTupleGenerateOperator<T>
{
  //one instance of TupleCacheOutputOperator map to one
  private static Map<String, List<?>> emittedTuplesMap = new HashMap<>();

  private String uuid;

  public TupleGenerateCacheOperator()
  {
    uuid = java.util.UUID.randomUUID().toString();
  }

  @SuppressWarnings("unchecked")
  protected void tupleEmitted( T tuple )
  {
    List<T> emittedTuples = (List<T>)emittedTuplesMap.get(uuid);
    if ( emittedTuples == null ) {
      emittedTuples = new ArrayList<T>();
      emittedTuplesMap.put(uuid, emittedTuples);
    }
    emittedTuples.add(tuple);
  }

  @SuppressWarnings("unchecked")
  public List<T> getTuples()
  {
    return (List<T>)emittedTuplesMap.get(uuid);
  }
}
