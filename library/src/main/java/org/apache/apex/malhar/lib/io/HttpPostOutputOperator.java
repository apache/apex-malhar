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

import javax.ws.rs.core.MediaType;

import org.codehaus.jettison.json.JSONObject;

import com.sun.jersey.api.client.WebResource;

import com.datatorrent.api.Context.OperatorContext;

/**
 * Sends tuple as POST with JSON content to the given URL.
 * <p>
 * Data of type {@link java.util.Map} is converted to JSON. All other types are sent in their {@link Object#toString()} representation.<br>
 * <br>
 * </p>
 * @displayName HTTP POST Output
 * @category Output
 * @tags http, post, output operator
 *
 * @param <T>
 * @since 0.3.2
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class HttpPostOutputOperator<T> extends AbstractHttpOperator<T>
{
  protected transient WebResource resource;

  @Override
  protected void processTuple(T t)
  {
    if (t instanceof Map) {
      resource.type(MediaType.APPLICATION_JSON).post(new JSONObject((Map<?, ?>)t).toString());
    } else {
      resource.post(t.toString());
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    resource = wsClient.resource(url);
  }

  private static final long serialVersionUID = 201405151144L;
}
