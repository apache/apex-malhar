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

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.appdata.schemas.Query;

public class SimpleDataValidatorTest
{
  @Test
  public void testValidatingQuery()
  {
    TestQuery testQuery = new TestQuery("1");
    SimpleDataValidator sqv = new SimpleDataValidator();

    Assert.assertTrue("The query object is not valid.", sqv.validate(testQuery, null));
  }

  @MessageType(type = TestQuery.TYPE)
  @MessageDeserializerInfo(clazz = SimpleDataDeserializer.class)
  @MessageValidatorInfo(clazz = SimpleDataValidator.class)
  public static class TestQuery extends Query
  {
    public static final String TYPE = "testQuery";

    public TestQuery(String id)
    {
      super(id);
    }
  }
}
