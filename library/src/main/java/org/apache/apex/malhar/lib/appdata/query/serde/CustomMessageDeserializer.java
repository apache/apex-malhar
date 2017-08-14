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
package org.apache.apex.malhar.lib.appdata.query.serde;

import java.io.IOException;

import org.apache.apex.malhar.lib.appdata.schemas.Message;

/**
 * This is an interface for a message deserializer. Classes implementing this interface should have a public
 * no-arg constructor.
 * @since 3.0.0
 */
public interface CustomMessageDeserializer
{
  /**
   * This method is called to convert a json string into a Message object.
   * @param json The JSON to deserialize.
   * @param message The Class object of the type of message to deserialize to.
   * @param context Any additional contextual information required to deserialize the message.
   * @return The deserialized message.
   * @throws IOException
   */
  Message deserialize(String json, Class<? extends Message> message, Object context) throws IOException;
}
