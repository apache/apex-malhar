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

import java.util.Map;
import java.util.Map.Entry;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

/**
 * Operator to take in map of key value pairs and make a HTTP GET request with the key value pairs from the map
 * as query parameters in the request.
 * <p>
 * If output port is connected, the response is emitted as {@link String} through the output port.
 * </p>
 * @displayName HTTP GET Map
 * @category Input
 * @tags http
 *
 * @param <K> Type of key in input map tuple
 * @param <V> Type of value in input map tuple
 * @since 1.0.2
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class HttpGetMapOperator<K, V> extends AbstractHttpGetOperator<Map<K, V>, String>
{
  @Override
  protected WebResource getResourceWithQueryParams(Map<K, V> t)
  {
    WebResource wr = wsClient.resource(url);

    for (Entry<K, V> entry : t.entrySet()) {
      wr = wr.queryParam(entry.getKey().toString(), entry.getValue().toString());
    }

    return wr;
  }

  @Override
  protected void processResponse(ClientResponse response)
  {
    output.emit(response.getEntity(String.class));
  }

  private static final long serialVersionUID = 201405151104L;
}
