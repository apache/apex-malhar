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

import javax.ws.rs.core.MultivaluedMap;

import com.sun.jersey.api.client.WebResource;

/**
 * Abstract Http get operator to get multi valued map from incoming tuple and create the web resource with query params
 *
 * @param <INPUT>
 * @param <OUTPUT>
 */
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
