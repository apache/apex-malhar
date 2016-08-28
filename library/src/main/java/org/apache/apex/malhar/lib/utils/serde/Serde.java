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

/**
 * This is an interface for a Serializer/Deserializer class.
 * @param <OBJ> The type of the object to Serialize and Deserialize.
 * @param <SER> The type to Serialize an Object to.
 *
 * @since 3.4.0
 */
public interface Serde<OBJ, SER> extends Serializer<OBJ, SER>, Deserializer<OBJ, SER>
{
}
