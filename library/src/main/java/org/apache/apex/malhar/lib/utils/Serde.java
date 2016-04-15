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
package org.apache.apex.malhar.lib.utils;

import org.apache.commons.lang3.mutable.MutableInt;

/**
 * This is an interface for a Serializer/Deserializer class.
 * @param <OBJ> The type of the object to Serialize and Deserialize.
 * @param <SER> The type to Serialize an Object to.
 */
public interface Serde<OBJ, SER>
{
  /**
   * Serialized the given object.
   * @param object The object to serialize.
   * @return The serialized representation of the object.
   */
  SER serialize(OBJ object);

  /**
   * Deserializes the given serialized representation of an object.
   * @param object The serialized representation of an object.
   * @param offset An offset in the serialized representation of the object. After the
   * deserialize method completes the offset is updated, so that the offset points to
   * the remaining unprocessed portion of the serialized object. For example:<br/>
   * {@code
   * Object obj;
   * MutableInt mi;
   * someObj1 = deserialize(obj, mi);
   * someObj2 = deserialize(obj, mi);
   * }
   *
   * @return The deserialized object.
   */
  OBJ deserialize(SER object, MutableInt offset);

  /**
   * Deserializes the given serialized representation of an object.
   * @param object The serialized representation of an object.
   *
   * @return The deserialized object.
   */
  OBJ deserialize(SER object);
}
