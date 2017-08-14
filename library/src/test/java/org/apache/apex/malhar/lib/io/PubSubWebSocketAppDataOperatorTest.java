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
package org.apache.apex.malhar.lib.io;

import java.lang.reflect.Method;
import java.net.URI;

import org.junit.Assert;
import org.junit.Test;
import org.apache.apex.malhar.lib.utils.PubSubHelper;

import com.datatorrent.common.experimental.AppData;

public abstract class PubSubWebSocketAppDataOperatorTest
{
  public static final String GATEWAY_CONNECT_ADDRESS_STRING = "my.gateway.com";
  public static final String URI_ADDRESS_STRING = "localhost:6666";
  public static final URI GATEWAY_CONNECT_ADDRESS;
  public static final URI URI_ADDRESS;

  static {
    try {
      GATEWAY_CONNECT_ADDRESS = PubSubHelper.getURI(GATEWAY_CONNECT_ADDRESS_STRING);
      URI_ADDRESS = PubSubHelper.getURI(URI_ADDRESS_STRING);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public abstract AppData.ConnectionInfoProvider getOperator();

  @Test
  public void testGetAppDataURL() throws Exception
  {
    String topic = "test";
    String correct = "pubsub";

    AppData.ConnectionInfoProvider pubsub = getOperator();

    setUri(pubsub, URI_ADDRESS);
    setTopic(pubsub, topic);

    Assert.assertEquals("The url is incorrect.", correct, pubsub.getAppDataURL());
  }

  public void setUri(Object o, URI uri) throws Exception
  {
    Class<?> clazz = o.getClass();
    Method m = clazz.getMethod("setUri", URI.class);
    m.invoke(o, uri);
  }

  public void setTopic(Object o, String topic) throws Exception
  {
    Class<?> clazz = o.getClass();
    Method m = clazz.getMethod("setTopic", String.class);
    m.invoke(o, topic);
  }
}
