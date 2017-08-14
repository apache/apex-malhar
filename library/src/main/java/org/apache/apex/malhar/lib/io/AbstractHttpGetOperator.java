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

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * This is the base implementation of an HTTP get operator,
 * which extracts query parameters from incoming tuples.&nbsp;
 * The operator then makes GET calls using the extracted parameters and given url.&nbsp;
 * Subclasses should implement the methods required to extract query parameters and process responses.
 * <p>
 * If the output port is connected, the response is processed
 * </p>
 * @displayName Abstract HTTP Query Input
 * @category Input
 * @tags http, input operator
 *
 * @param <INPUT> tuple type of input port
 * @param <OUTPUT> tuple type of output port
 * @since 1.0.2
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class AbstractHttpGetOperator<INPUT, OUTPUT> extends AbstractHttpOperator<INPUT>
{
  /**
   * The output port which emits retrieved tuples.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<OUTPUT> output = new DefaultOutputPort<OUTPUT>();

  @Override
  protected void processTuple(INPUT t)
  {
    WebResource wr = getResourceWithQueryParams(t);
    if (output.isConnected()) {
      ClientResponse response = wr.get(ClientResponse.class);
      processResponse(response);
    } else {
      wr.get(ClientResponse.class);
    }
  }

  protected abstract WebResource getResourceWithQueryParams(INPUT t);

  protected abstract void processResponse(ClientResponse response);

}
