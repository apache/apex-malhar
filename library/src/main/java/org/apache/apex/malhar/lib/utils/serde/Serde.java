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
package org.apache.apex.malhar.lib.utils.serde;

import org.apache.commons.lang3.mutable.MutableInt;

/**
 * This is an interface for a Serializer/Deserializer class.
 * @param <T> The type of the object to Serialize and Deserialize.
 *
 * @since 3.4.0
 */
public interface Serde<T>
{
  /**
   * Serialize the object to the input SerializeBuffer. This give the chance of share same SerializeBuffer with different object.
   * @param object
   * @param buffer
   */
  void serialize(T object, SerializationBuffer buffer);

  /**
   * Deserialize from the buffer and return a new object.
   * After the deserialize method completes the offset is updated, so that the offset points to
   * the remaining unprocessed portion of the serialization buffer.
   *
   * @param buffer
   * @param offset An offset of the buffer.
   * @param length The input length
   * @return
   */
  T deserialize(byte[] buffer, MutableInt offset, int length);
}
