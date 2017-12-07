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
package org.apache.apex.malhar.lib.utils;

import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.base.Preconditions;

import com.datatorrent.api.Context;

@InterfaceStability.Evolving
public class PubSubHelper
{
  private static final Logger logger = LoggerFactory.getLogger(PubSubHelper.class);

  public static boolean isGatewayConfigured(Context context)
  {
    return context.getValue(Context.DAGContext.GATEWAY_CONNECT_ADDRESS) != null;
  }

  public static URI getURI(Context context)
  {
    return getURIWithDefault(context, null);
  }

  public static URI getURIWithDefault(Context context, String defaultAddress)
  {
    String address = context.getValue(Context.DAGContext.GATEWAY_CONNECT_ADDRESS);
    if (address == null) {
      address = defaultAddress;
    }
    return getURI(address, context.getValue(Context.DAGContext.GATEWAY_USE_SSL));
  }

  public static URI getURI(String address)
  {
    return getURI(address, false);
  }

  public static URI getURI(String address, boolean useSSL)
  {
    Preconditions.checkNotNull(address,"No address specified");
    String uri = (useSSL ? "wss://" : "ws://") + address + "/pubsub";
    logger.debug("PubSub uri {}", uri);
    return URI.create(uri);
  }
}
