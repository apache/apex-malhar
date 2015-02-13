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

import com.datatorrent.api.AppDataOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.util.PubSubMessage;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.http.client.utils.URIBuilder;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubWebSocketAppDataQuery extends PubSubWebSocketInputOperator<String> implements AppDataOperator
{
  /**
   * Add optional error port
   * 
   */
  private static final Logger logger = LoggerFactory.getLogger(PubSubWebSocketAppDataQuery.class);

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

    logger.debug(message);

    try {
      data = new JSONObject(message).getString(PubSubMessage.DATA_KEY);
    }
    catch(JSONException e) {
      return null;
    }

    logger.debug(data);
    return data;
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
}
