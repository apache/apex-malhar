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


import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;

import com.datatorrent.common.experimental.AppData;
import com.datatorrent.common.util.PubSubMessage;

/**
 * This is an app data pub sub query operator.
 *
 * @displayName App Data Pub Sub Query
 * @category input
 * @tags input, appdata, query
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
    super.setup(context);

    logger.debug("Setting up:\nuri:{}\ntopic:{}",this.getUri(), this.getTopic());
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
