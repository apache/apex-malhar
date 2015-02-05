/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.io;

import com.datatorrent.api.AppDataTopicResultOperator;
import com.datatorrent.lib.util.PubSubWebSocketClient;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.http.client.utils.URIBuilder;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class PubSubWebSocketAppDataResult extends PubSubWebSocketOutputOperator<String> implements AppDataTopicResultOperator
{
  private boolean appendQIDToTopic = true;

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

    return PubSubWebSocketClient.constructPublishMessage(getTopic() + id, t, codec);
  }
}
