/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.ShipContainingJars;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

/**
 * Reads via GET from given URL as input stream<p>
 * <br>
 *
 * @since 0.3.2
 */
@ShipContainingJars(classes = {com.sun.jersey.api.client.ClientHandler.class})
public abstract class AbstractHttpInputOperator<T> extends SimpleSinglePortInputOperator<T> implements Runnable
{
  public final transient DefaultOutputPort<String> rawOutput = new DefaultOutputPort<String>();
  private static final Logger LOG = LoggerFactory.getLogger(AbstractHttpInputOperator.class);
  /**
   * Timeout interval for reading from server. 0 or negative indicates no timeout.
   */
  public int readTimeoutMillis = 0;
  /**
   * The URL of the web service resource for the POST request.
   */
  @NotNull
  private URI resourceUrl;
  private Map<String, String> headers = new HashMap<String, String>();
  private transient Client wsClient;
  private transient WebResource resource;

  public void setUrl(URI u)
  {
    resourceUrl = u;
  }

  public void setHeader(String key, String value)
  {
    headers.put(key, value);
  }

  @Override
  public void setup(OperatorContext context)
  {
    wsClient = Client.create();
    wsClient.setFollowRedirects(true);
    wsClient.setReadTimeout(readTimeoutMillis);
    resource = wsClient.resource(resourceUrl.toString()); // side step "not absolute URL" after serialization
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

  public abstract void processResponse(ClientResponse response) throws IOException;

  @Override
  public void run()
  {
    while (super.isActive()) {
      try {
        WebResource.Builder builder = resource.getRequestBuilder();

        for (Map.Entry<String, String> entry : headers.entrySet()) {
          builder = builder.header(entry.getKey(), entry.getValue());
        }

        ClientResponse response = builder.get(ClientResponse.class);
        processResponse(response);
      }
      catch (Exception e) {
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
