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

import java.io.IOException;
import java.net.URI;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.experimental.AppData;
import com.datatorrent.common.util.PubSubMessage.PubSubMessageType;

/**
 * This is an app data pub sub result operator. This operator is used to send results to
 * App Data dashboards produced by App Data store operators.
 *
 * @displayName App Data Pub Sub Result
 * @category DT View Integration
 * @tags output, app data, result
 * @since 3.0.0
 */
@AppData.AppendQueryIdToTopic(value = true)
public class PubSubWebSocketAppDataResult extends PubSubWebSocketOutputOperator<String>
    implements AppData.ConnectionInfoProvider
{
  private static final Logger logger = LoggerFactory.getLogger(PubSubWebSocketAppDataResult.class);

  public PubSubWebSocketAppDataResult()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    setUri(PubSubWebSocketAppDataQuery.uriHelper(context, getUri()));
    logger.debug("Setting up:\nuri:{}\ntopic:{}",this.getUri(), this.getTopic());
    super.setup(context);
  }

  @Override
  public String getAppDataURL()
  {
    return "pubsub";
  }

  /**
   * Gets the URI for WebSocket connection.
   *
   * @return the URI
   */
  @Override
  public URI getUri()
  {
    return super.getUri();
  }

  /**
   * The URI for WebSocket connection. If this is not set, the value of the dt.attr.GATEWAY_CONNECT_ADDRESS DAG attribute is used. If neither this
   * property or dt.attr.GATEWAY_CONNECT_ADDRESS attribute is set, then this operator will fail with an {@link IllegalArgumentException}.
   *
   * @param uri
   */
  @Override
  public void setUri(URI uri)
  {
    super.setUri(uri);
  }

  @Override
  public String convertMapToMessage(String t) throws IOException
  {
    JSONObject jo = null;

    try {
      jo = new JSONObject(t);
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }

    String topic = getTopic();

    if (jo.has("id")) {
      try {
        topic += "." + jo.getString("id");
      } catch (JSONException ex) {
        throw new RuntimeException(ex);
      }
    }

    JSONObject output = new JSONObject();

    try {
      output.put("topic", topic);
      output.put("data", jo);
      output.put("type", PubSubMessageType.PUBLISH.getIdentifier());
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }

    logger.debug("Output json {}", output.toString());
    logger.debug("Writing to topic: {}", topic);

    return output.toString();
  }
}
