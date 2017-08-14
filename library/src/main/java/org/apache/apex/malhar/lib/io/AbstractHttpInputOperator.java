/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.io;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;

/**
 * This is a base implementation for an HTTP input operator that reads from a given url using the HTTP GET command like an input stream.&nbsp;
 * Subclasses must implement the method which handles the response to the HTTP GET request.
 * <p></p>
 * @displayName Abstract HTTP Input
 * @category Input
 * @tags http, input operator
 *
 * @since 0.3.2
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class AbstractHttpInputOperator<T> extends SimpleSinglePortInputOperator<T> implements Runnable
{
  /**
   * The output port which emits retrieved tuples.
   */
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
  private URI url;
  private Map<String, String> headers = new HashMap<String, String>();
  private transient Client wsClient;
  private transient WebResource resource;

  /**
   * The url to read from.
   * @param u The url to read from.
   */
  public void setUrl(URI u)
  {
    url = u;
  }

  /**
   * Sets the url to read from.
   * @return The url to read from.
   */
  public URI getUrl()
  {
    return url;
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
    resource = wsClient.resource(url.toString()); // side step "not absolute URL" after serialization
    LOG.info("URL: {}", url);
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
      } catch (Exception e) {
        LOG.error("Error reading from " + resource.getURI(), e);
      }

      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        LOG.info("Exiting IO loop {}.", e.toString());
        break;
      }
    }
  }

}
