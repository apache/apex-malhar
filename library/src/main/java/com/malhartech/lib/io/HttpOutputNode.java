/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.annotation.ShipContainingJars;
import com.malhartech.dag.AbstractModule;
import com.malhartech.dag.Component;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.ModuleConfiguration;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.MissingResourceException;
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
@ShipContainingJars(classes={com.sun.jersey.api.client.ClientHandler.class})
@ModuleAnnotation(
    ports = {
        @PortAnnotation(name = Component.INPUT, type = PortType.INPUT)
    }
)
public class HttpOutputNode extends AbstractModule
{
  private static final Logger LOG = LoggerFactory.getLogger(HttpOutputNode.class);

  /**
   * The URL of the web service resource for the POST request.
   */
  public static final String P_RESOURCE_URL = "resourceUrl";

  private transient URI resourceUrl;
  private transient Client wsClient;
  private transient WebResource resource;

  @Override
  public void setup(ModuleConfiguration config) throws FailedOperationException
  {
    try {
      checkConfiguration(config);
    }
    catch (Exception ex) {
      throw new FailedOperationException(ex);
    }

    wsClient = Client.create();
    wsClient.setFollowRedirects(true);
    resource = wsClient.resource(resourceUrl);
    LOG.info("URL: {}", resourceUrl);
  }

  @Override
  public void teardown() {
    if (wsClient != null) {
      wsClient.destroy();
    }
    super.teardown();
  }

  @Override
  public boolean checkConfiguration(ModuleConfiguration config) {
    String urlStr = config.get(P_RESOURCE_URL);
    if (urlStr == null) {
      throw new MissingResourceException("Key for URL string not set", String.class.getSimpleName(), P_RESOURCE_URL);
    }
    try {
      this.resourceUrl = new URI(urlStr);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(String.format("Invalid value '%s' for '%s'.", urlStr, P_RESOURCE_URL));
    }
    return true;
  }

  /**
   *
   * @param t the value of t
   */
  @Override
  public void process(Object t)
  {
    try {
      if (t instanceof Map) {
        resource.type(MediaType.APPLICATION_JSON).post(new JSONObject((Map<?,?>)t));
      } else {
        resource.post(""+t);
      }
    }
    catch (Exception e) {
      LOG.error("Failed to send tuple to " + resource.getURI());
    }
  }

}
