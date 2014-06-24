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

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * Http get operator to extract query parameters from incoming tuple and make GET calls to
 * the set url.
 *
 * If the output port is connected, the response is processed
 *
 * @param <INPUT> tuple type of input port
 * @param <OUTPUT> tuple type of output port
 */
public abstract class AbstractHttpGetOperator<INPUT, OUTPUT> extends AbstractHttpOperator<INPUT>
{
  @OutputPortFieldAnnotation(name = "output", optional = true)
  public final transient DefaultOutputPort<OUTPUT> output = new DefaultOutputPort<OUTPUT>();

  @Override
  protected void processTuple(INPUT t)
  {
    WebResource wr = getResourceWithQueryParams(t);
    if (output.isConnected()) {
      ClientResponse response = wr.get(ClientResponse.class);
      processResponse(response);
    }
    else {
      wr.get(ClientResponse.class);
    }
  }

  protected abstract WebResource getResourceWithQueryParams(INPUT t);

  protected abstract void processResponse(ClientResponse response);

}
