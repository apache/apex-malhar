/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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

import com.datatorrent.api.AppDataTopicResultOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.util.PubSubMessage.PubSubMessageType;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.http.client.utils.URIBuilder;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubWebSocketAppDataResult extends PubSubWebSocketOutputOperator<String> implements AppDataTopicResultOperator
{
  private static final Logger logger = LoggerFactory.getLogger(PubSubWebSocketAppDataResult.class);

  private boolean appendQIDToTopic = true;

  public PubSubWebSocketAppDataResult()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    logger.debug("Setting up: ");
  }

  @Override
  public boolean isAppendQIDToTopic()
  {
    return appendQIDToTopic;
  }

  @Override
  public void setAppendQIDToTopic(boolean appendQIDToTopic)
  {
    this.appendQIDToTopic = appendQIDToTopic;
  }

  @Override
  public URI getAppDataURL()
  {
    URIBuilder ub = new URIBuilder(this.getUri());
    ub.addParameter("topic", getTopic());

    URI uri;

    try {
      uri = ub.build();
    }
    catch(URISyntaxException ex) {
      throw new RuntimeException(ex);
    }

    return uri;
  }

  @Override
  public String convertMapToMessage(String t) throws IOException
  {
    JSONObject jo = null;

    try {
      jo = new JSONObject(t);
    }
    catch(JSONException ex) {
      throw new RuntimeException(ex);
    }

    String id;

    try {
      id = jo.getString("id");
    }
    catch(JSONException ex) {
      throw new RuntimeException(ex);
    }

    String topic = getTopic();

    if(appendQIDToTopic) {
      topic += id;
    }

    JSONObject output = new JSONObject();

    try {
      output.put("topic", topic);
      output.put("data", jo);
      output.put("type", PubSubMessageType.PUBLISH.getIdentifier());
    }
    catch(JSONException ex) {
      throw new RuntimeException(ex);
    }

    logger.debug("Writing to topic: {}", topic);

    return output.toString();
  }
}
