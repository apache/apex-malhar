/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.ShipContainingJars;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONObject;

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

  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T t)
    {
      try {
        if (t instanceof Map) {
          resource.post(JsonStringGenerator.createJsonString((Map)t));
        } else {
         resource.post("" + t);
        }
      }
      catch (Exception e) {
        LOG.error("Failed to send tuple to {} {}", resource.getURI(), e.getMessage());
        System.out.println("Failed to send tuple to " + resource.getURI() + " " + e.getMessage());
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
    resource = wsClient.resource(resourceUrl); // side step "not absolute URL" after serialization
    LOG.info("URL: {}", resourceUrl);
    System.out.println("URL: {} " + resourceUrl );
  }

  @Override
  public void teardown()
  {
    if (wsClient != null) {
      wsClient.destroy();
    }
    super.teardown();
  }
  
  @SuppressWarnings("unused")
private static class JsonStringGenerator {
    /**
     * Method to convert map into json format
     * @param map with data to be converted into json
     * @return json string
     */
    @SuppressWarnings({ "rawtypes" })
    public static String createJsonString(Map jsonMap) throws IOException {

     StringWriter writer = new StringWriter();
     JsonGenerator jsonGenerator = new JsonFactory().
           createJsonGenerator(writer);
     ObjectMapper mapper = new ObjectMapper();
     mapper.writeValue(jsonGenerator, jsonMap);
     jsonGenerator.close();
     System.out.println(writer.toString());
     return writer.toString();
    }
    
   }
}
