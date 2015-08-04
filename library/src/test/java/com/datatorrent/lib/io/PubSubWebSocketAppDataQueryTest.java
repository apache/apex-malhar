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
package com.datatorrent.lib.io;

import java.net.URI;
import org.junit.Assert;
import org.junit.Test;

public class PubSubWebSocketAppDataQueryTest
{
  @Test
  public void testGetAppDataURL() throws Exception
  {
    URI uri = URI.create("ws://localhost:6666/pubsub");
    String topic = "test";
    String correct = "pubsub";

    PubSubWebSocketAppDataQuery pubsub = new PubSubWebSocketAppDataQuery();
    pubsub.setUri(uri);
    pubsub.setTopic(topic);

    Assert.assertEquals("The url is incorrect.", correct, pubsub.getAppDataURL());
  }
}
