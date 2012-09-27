/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.ws.rs.core.MediaType;

import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.annotation.ShipContainingJars;
import com.malhartech.dag.AbstractInputModule;
import com.malhartech.dag.Component;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.ModuleConfiguration;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

/**
 *
 * Reads data via GET from given URL<p>
 * <br>
 * Data of type {@link java.util.Map} is converted to JSON. All other types are sent in their {@link Object#toString()} representation.<br>
 * <br>
 *
 */
@ShipContainingJars(classes={com.sun.jersey.api.client.ClientHandler.class})
@ModuleAnnotation(
    ports = {
        @PortAnnotation(name = Component.OUTPUT, type = PortType.OUTPUT)
    }
)
public class HttpInputModule extends AbstractInputModule
{
  private static final Logger LOG = LoggerFactory.getLogger(HttpInputModule.class);

  /**
   * The URL of the web service resource for the POST request.
   */
  public static final String P_RESOURCE_URL = "resourceUrl";

  private transient URI resourceUrl;
  private transient Client wsClient;
  private transient WebResource resource;
  private transient Thread ioThread;

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

    // launch IO thread
    Runnable r = new Runnable() {
      @Override
      public void run() {
        HttpInputModule.this.run();
      }
    };
    this.ioThread = new Thread(r, "http-io-"+this.getId());
    this.ioThread.start();

  }

  @Override
  public void teardown() {
    this.ioThread.interrupt();
    if (wsClient != null) {
      wsClient.destroy();
    }
    super.teardown();
  }

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

  @Override
  public void process(Object payload) {
    Object tuple;
    while ((tuple = tuples.poll()) != null) {
      emit(Component.OUTPUT, tuple);
    }
  }

  private final ConcurrentLinkedQueue<Object> tuples = new ConcurrentLinkedQueue<Object>();

  //@Override
  public void run() {
    while (true) {
      try {
        ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        if (!MediaType.APPLICATION_JSON_TYPE.equals(response.getType())) {
          LOG.error("Unexpected response type " + response.getType());
        } else {
          // we are expecting a JSON object and converting one level of keys to a map, no further mapping is performed
          JSONObject json = response.getEntity(JSONObject.class);
          Map<String, Object> tuple = new HashMap<String, Object>();
          Iterator<?> it = json.keys();
          while (it.hasNext()) {
            String key = (String)it.next();
            Object val = json.get(key);
            if (val != null) {
              tuple.put(key, val);
            }
          }
          if (!tuple.isEmpty()) {
            LOG.debug("Got: " + tuple);
            tuples.offer(tuple);
          }
        }
      } catch (Exception e) {
          LOG.error("Error reading from " + resource.getURI(), e);
      }
      try {
        Thread.sleep(500);
      }
      catch (InterruptedException e) {
        LOG.info("Exiting IO loop {}.", e.toString());
        break;
      }
    }
  }

}
