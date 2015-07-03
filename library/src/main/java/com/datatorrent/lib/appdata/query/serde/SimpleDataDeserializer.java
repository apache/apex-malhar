/*
 * Copyright (c) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.appdata.query.serde;

import java.io.IOException;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.appdata.schemas.Message;

/**
 * This is a simple Message deserializer which deserializes messages using Jackson.
 */
public class SimpleDataDeserializer implements CustomMessageDeserializer
{
  private ObjectMapper om = new ObjectMapper();

  /**
   * No-arg constructor is requried by {@link CustomMessageDeserializer} interface.
   */
  public SimpleDataDeserializer()
  {
  }

  @Override
  public Message deserialize(String json, Class<? extends Message> clazz, Object context) throws IOException
  {
    Message data;

    try {
      data = om.readValue(json, clazz);
    }
    catch(IOException ex) {
      throw ex;
    }

    return data;
  }

  private static final Logger LOG = LoggerFactory.getLogger(SimpleDataDeserializer.class);
}
