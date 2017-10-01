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
package org.apache.apex.malhar.stream.api;



/**
 * Created by vikram on 7/6/17.
 */
public interface PythonApexStream<T> extends ApexStream<T>
{

  /**
   * Add custom serialized Python Function along with options
   * @param serializedFunction stores Serialized Function data
   * @return new stream of type T
   */
  <STREAM extends PythonApexStream<T>> STREAM map(byte[] serializedFunction, Option... opts);

  /**
   * Add custom serialized Python Function along with options
   * @param serializedFunction stores Serialized Function data
   * @return new stream of type T
   */
  <STREAM extends PythonApexStream<T>> STREAM flatMap(byte[] serializedFunction, Option... opts);

  /**
   * Add custom serialized Python Function along with options
   * @param serializedFunction stores Serialized Function data
   * @return new stream of type T
   */
  <STREAM extends PythonApexStream<T>> STREAM filter(byte[] serializedFunction, Option... opts);

}
