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

import javax.ws.rs.core.MultivaluedMap;

import com.sun.jersey.api.client.WebResource;

/**
 * This is the base implementation of an HTTP get operator,
 * which extracts query parameters from an incoming tuple and places the parameters in a multivalued map.&nbsp;
 * Subclasses should implement the method used to convert tuples into query parameters.
 * <p></p>
 * @displayName Abstract HTTP Multivalued Query Input
 * @category Input
 * @tags http, input operator
 *
 * @param <INPUT>
 * @param <OUTPUT>
 * @since 1.0.2
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class AbstractHttpGetMultiValuedMapOperator<INPUT, OUTPUT> extends AbstractHttpGetOperator<INPUT, OUTPUT>
{
  @Override
  protected WebResource getResourceWithQueryParams(INPUT t)
  {
    WebResource wr = wsClient.resource(url);
    wr = wr.queryParams(getQueryParams(t));

    return wr;
  }

  protected abstract MultivaluedMap<String, String> getQueryParams(INPUT input);

}
