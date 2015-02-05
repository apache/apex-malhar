/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.io;

import com.datatorrent.api.AppDataOperator;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.http.client.utils.URIBuilder;

/**
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class PubSubWebSocketAppDataQuery extends PubSubWebSocketInputOperator<String> implements AppDataOperator
{
  PubSubWebSocketAppDataQuery()
  {
  }

  @Override
  protected String convertMessage(String message)
  {
    return message;
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
