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
import java.net.URISyntaxException;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;

import com.datatorrent.common.experimental.AppData;
import com.datatorrent.common.util.PubSubMessage;

/**
 * This is an App Data pub sub query operator. This operator is used to receive queries from
 * App Data dashboards and forward queries to App Data store operators.
 *
 * @displayName App Data Pub Sub Query
 * @category DT View Integration
 * @tags input, app data, query
 */
public class PubSubWebSocketAppDataQuery extends PubSubWebSocketInputOperator<String> implements AppData.ConnectionInfoProvider
{
  private static final Logger logger = LoggerFactory.getLogger(PubSubWebSocketAppDataQuery.class);

  private static final long serialVersionUID = 201506121124L;

  public PubSubWebSocketAppDataQuery()
  {
    this.skipNull = true;
  }

  @Override
  public void setup(OperatorContext context)
  {
    this.uri = uriHelper(context, uri);
    logger.debug("Setting up:\nuri:{}\ntopic:{}",this.getUri(), this.getTopic());
    super.setup(context);
  }

  public static URI uriHelper(OperatorContext context, URI uri)
  {
    if (uri == null) {
      if (context.getValue(DAG.GATEWAY_CONNECT_ADDRESS) == null) {
        throw new IllegalArgumentException("The uri property is not set and the dt.attr.GATEWAY_CONNECT_ADDRESS is not defined");
      }

      try {
        uri = new URI("ws://"
                      + context.getValue(DAG.GATEWAY_CONNECT_ADDRESS)
                      + "/pubsub");
      } catch (URISyntaxException ex) {
        throw new RuntimeException(ex);
      }
    }

    return uri;
  }

  /**
   * Gets the URI for WebSocket connection.
   *
   * @return the URI
   */
  @Override
  public URI getUri()
  {
    return uri;
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
    this.uri = uri;
  }

  @Override
  protected String convertMessage(String message)
  {
    String data;

    try {
      JSONObject jo = new JSONObject(message);
      JSONArray ja = jo.names();

      //Make sure that only the correct keys are in the first level of JSON
      for(int keyIndex = 0;
          keyIndex < ja.length();
          keyIndex++) {
        String key = ja.getString(keyIndex);
        if(!(PubSubMessage.DATA_KEY.equals(key) ||
           PubSubMessage.TOPIC_KEY.equals(key) ||
           PubSubMessage.TYPE_KEY.equals(key))) {
          logger.error("{} is not a valid key in the first level of the following pubsub message:\n{}",
                       key,
                       message);
          return null;
        }
      }

      data = jo.getString(PubSubMessage.DATA_KEY);
    }
    catch(JSONException e) {
      return null;
    }

    return data;
  }

  @Override
  public String getAppDataURL()
  {
    return "pubsub";
  }
}
