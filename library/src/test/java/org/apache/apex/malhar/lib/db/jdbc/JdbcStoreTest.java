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
package org.apache.apex.malhar.lib.db.jdbc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class JdbcStoreTest
{
  @Test
  public void testConnectionPropertiesSetup()
  {
    JdbcStore store = new JdbcStore();
    store.setConnectionProperties("user:test,password:pwd");
    Properties properties = store.getConnectionProperties();
    Assert.assertEquals("user", properties.get("user"), "test");
    Assert.assertEquals("password", properties.get("password"), "pwd");
  }

  @Test
  public void testStoreSerialization()
  {
    Kryo kryo = new Kryo();
    JdbcStore store = new JdbcStore();
    store.setConnectionProperties("user:test,password:pwd");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output output = new Output(baos);
    kryo.writeClassAndObject(output, store);
    output.flush();
    byte[] bytes = baos.toByteArray();

    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    Input input = new Input(bais);
    JdbcStore deserializedStrore = (JdbcStore)kryo.readClassAndObject(input);
    Assert.assertEquals("connection properties", store.getConnectionProperties(),
        deserializedStrore.getConnectionProperties());
  }
}
