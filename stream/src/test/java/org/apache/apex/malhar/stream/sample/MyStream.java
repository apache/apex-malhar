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
package org.apache.apex.malhar.stream.sample;

import org.apache.apex.malhar.lib.function.Function;
import org.apache.apex.malhar.stream.api.impl.ApexStreamImpl;

import com.datatorrent.api.DAG;

/**
 * An example to create your own stream
 */
public class MyStream<T> extends ApexStreamImpl<T>
{

  public MyStream(ApexStreamImpl<T> apexStream)
  {
    super(apexStream);
  }

  <O> MyStream<O> myFilterAndMap(Function.MapFunction<T, O> map, Function.FilterFunction<T> filterFunction)
  {
    return filter(filterFunction).map(map).with(DAG.Locality.THREAD_LOCAL);
  }

}
