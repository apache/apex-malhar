/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io;

import java.io.IOException;

import javax.validation.constraints.NotNull;

import com.datatorrent.common.util.PubSubMessageCodec;
import com.datatorrent.common.util.PubSubWebSocketClient;

/**
 * This operator writes maps as JSON objects to the given URL.
 * <p></p>
 * @displayName Pub Sub Web Socket Output
 * @category Input
 * @tags http, input operator
 *
 * @param <T>
 * @since 0.3.2
 */
public class PubSubWebSocketOutputOperator<T> extends WebSocketOutputOperator<T>
{
  private String topic = null;
  private transient PubSubMessageCodec<Object> codec = new PubSubMessageCodec<Object>(mapper);

  public void setTopic(String topic)
  {
    this.topic = topic;
  }

  @NotNull
  public String getTopic()
  {
    return topic;
  }

  @Override
  public String convertMapToMessage(T t) throws IOException
  {
    return PubSubWebSocketClient.constructPublishMessage(topic, t, codec);
  }

}
