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
package org.apache.apex.malhar.contrib.geode;

import org.junit.Before;
import org.junit.Test;

import org.apache.apex.malhar.lib.db.KeyValueStoreOperatorTest;

public class GeodeOperatorTest
{
  KeyValueStoreOperatorTest<GeodeStore> testFramework;

  private GeodeStore geodeStore;
  private GeodeStore testStore;

  @Before
  public void setup()
  {

    geodeStore = new GeodeStore();
    testStore = new GeodeStore();

    geodeStore.setLocatorHost("192.168.1.128");
    geodeStore.setLocatorPort(50505);
    geodeStore.setRegionName("operator");

    testStore.setLocatorHost("192.168.1.128");
    testStore.setLocatorPort(50505);
    testStore.setRegionName("operator");

    if (System.getProperty("dev.locator.connection") != null) {
      geodeStore.setLocatorHost(System.getProperty("dev.locator.connection"));
      testStore.setLocatorHost(System.getProperty("dev.locator.connection"));
    }
    testFramework = new KeyValueStoreOperatorTest<GeodeStore>(geodeStore, testStore);

  }

  @Test
  public void testOutputOperator() throws Exception
  {
    testFramework.testOutputOperator();
  }

  @Test
  public void testInputOperator() throws Exception
  {
    testFramework.testInputOperator();

  }

}
