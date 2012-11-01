/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.ShipContainingJars;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultInputPort;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import java.net.URI;
import java.util.Map;
import javax.ws.rs.core.MediaType;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Sends tuple as POST with JSON content to the given URL<p>
 * <br>
 * Data of type {@link java.util.Map} is converted to JSON. All other types are sent in their {@link Object#toString()} representation.<br>
 * <br>
 *
 */
@ShipContainingJars(classes = {com.sun.jersey.api.client.ClientHandler.class})
public class HttpOutputOperator<T> extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(HttpOutputOperator.class);

  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>(this)
  {
    @Override
    public void process(T t)
    {
      try {
        if (t instanceof Map) {
          resource.type(MediaType.APPLICATION_JSON).post(new JSONObject((Map<?, ?>)t));
        }
        else {
          resource.post("" + t);
        }
      }
      catch (Exception e) {
        LOG.error("Failed to send tuple to " + resource.getURI());
      }
    }
  };

  /**
   * The URL of the web service resource for the POST request.
   */
  private URI resourceUrl;
  private transient Client wsClient;
  private transient WebResource resource;

  public void setResourceURL(URI url)
  {
    if (!url.isAbsolute()) {
      throw new IllegalArgumentException("URL is not absolute: " + url);
    }
    this.resourceUrl = url;
  }

  @Override
  public void setup(OperatorContext context)
  {
    wsClient = Client.create();
    wsClient.setFollowRedirects(true);
    resource = wsClient.resource(resourceUrl.toString());
    LOG.info("URL: {}", resourceUrl);
  }

  @Override
  public void teardown()
  {
    if (wsClient != null) {
      wsClient.destroy();
    }
    super.teardown();
  }
}
