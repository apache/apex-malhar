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

import java.io.IOException;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;

import com.datatorrent.common.experimental.AppData;
import com.datatorrent.common.util.PubSubMessage.PubSubMessageType;
/**
 * This is an app data pub sub result operator.
 *
 * @displayName App Data Pub Sub Result
 * @category outut
 * @tags output, appdata, result
 */
@AppData.AppendQueryIdToTopic(value=true)
public class PubSubWebSocketAppDataResult extends PubSubWebSocketOutputOperator<String> implements AppData.ConnectionInfoProvider
{
  private static final Logger logger = LoggerFactory.getLogger(PubSubWebSocketAppDataResult.class);

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
  public String getAppDataURL()
  {
    return "pubsub";
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

    String topic = getTopic() + "." + id;

    JSONObject output = new JSONObject();

    try {
      output.put("topic", topic);
      output.put("data", jo);
      output.put("type", PubSubMessageType.PUBLISH.getIdentifier());
    }
    catch(JSONException ex) {
      throw new RuntimeException(ex);
    }

    logger.debug("Output json {}", output.toString());
    logger.debug("Writing to topic: {}", topic);

    return output.toString();
  }
}
