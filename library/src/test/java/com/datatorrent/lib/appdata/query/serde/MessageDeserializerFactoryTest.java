/*
 * Copyright (c) 2015 DataTorrent, Inc.
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
package com.datatorrent.lib.appdata.query.serde;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.appdata.schemas.Message;
import com.datatorrent.lib.appdata.schemas.SchemaQuery;

public class MessageDeserializerFactoryTest
{
  @Test
  public void testMalformedQuery()
  {
    @SuppressWarnings("unchecked")
    MessageDeserializerFactory qdf = new MessageDeserializerFactory(SchemaQuery.class);

    String malformed = "{\"}";
    boolean exception = false;

    try {
      qdf.deserialize(malformed);
    }
    catch(IOException e) {
      exception = true;
    }

    Assert.assertTrue("Resulting query should throw IOException.", exception);
  }

  @Test
  public void testUnregisteredQueryType()
  {
    @SuppressWarnings("unchecked")
    MessageDeserializerFactory qdf = new MessageDeserializerFactory(SchemaQuery.class);

    String unsupportedQuery = "{\"id\":\"1\",\"type\":\"Invalid type\"}";
    boolean exception = false;

    Message data = null;

    try {
      data = qdf.deserialize(unsupportedQuery);
    }
    catch(IOException e) {
      exception = true;
    }

    Assert.assertTrue("Resulting query should be null.", exception);
  }

  private static final Logger LOG = LoggerFactory.getLogger(MessageDeserializerFactoryTest.class);
}
