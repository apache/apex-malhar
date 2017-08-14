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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.experimental.AppData.ConnectionInfoProvider;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

public class PubSubWebSocketAppDataQueryTest extends PubSubWebSocketAppDataOperatorTest
{
  private static OperatorContext context;
  private static OperatorContext emptyContext;

  @BeforeClass
  public static void setupContext() throws Exception
  {
    Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(Context.DAGContext.GATEWAY_CONNECT_ADDRESS, GATEWAY_CONNECT_ADDRESS_STRING);
    context = mockOperatorContext(1, attributes);

    attributes = new Attribute.AttributeMap.DefaultAttributeMap();
    emptyContext = mockOperatorContext(1, attributes);
  }

  @Override
  public ConnectionInfoProvider getOperator()
  {
    return new PubSubWebSocketAppDataQuery();
  }

  @Test
  public void testURISet() throws Exception
  {
    Assert.assertEquals(URI_ADDRESS, PubSubWebSocketAppDataQuery.uriHelper(emptyContext, URI_ADDRESS));
  }

  @Test
  public void testNoURISet() throws Exception
  {
    boolean threwException = false;

    try {
      PubSubWebSocketAppDataQuery.uriHelper(emptyContext, null);
    } catch (Exception e) {
      threwException = e instanceof IllegalArgumentException;
    }

    Assert.assertTrue(threwException);
  }

  @Test
  public void testAttrSet() throws Exception
  {
    Assert.assertEquals(GATEWAY_CONNECT_ADDRESS, PubSubWebSocketAppDataQuery.uriHelper(context, null));
  }

  @Test
  public void testAttrAndURISet() throws Exception
  {
    Assert.assertEquals(URI_ADDRESS, PubSubWebSocketAppDataQuery.uriHelper(context, URI_ADDRESS));
  }
}
