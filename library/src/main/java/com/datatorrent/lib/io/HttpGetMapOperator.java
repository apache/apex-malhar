/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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

import java.util.Map;
import java.util.Map.Entry;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

/**
 * Operator to take in map of key value pairs and make a HTTP GET request with the key value pairs from the map
 * as query parameters in the request.
 *
 * If output port is connected, the response is emitted as {@link String} through the output port.
 *
 * @param <K> Type of key in input map tuple
 * @param <V> Type of value in input map tuple
 */
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
