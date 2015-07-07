/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.memcache;

import java.net.InetSocketAddress;

import net.spy.memcached.AddrUtil;

import org.junit.Before;
import org.junit.Test;

import com.datatorrent.lib.db.KeyValueStoreOperatorTest;

/**
 *
 * @since 0.9.3
 */
public class MemcacheOperatorTest
{
  KeyValueStoreOperatorTest<MemcacheStore> testFramework;

  @Before
  public void setup()
  {
    MemcacheStore operatorStore = new MemcacheStore();
    operatorStore.setServerAddresses( AddrUtil.getAddresses("localhost:11211") );
    MemcacheStore testStore = new MemcacheStore();
    testStore.setServerAddresses( AddrUtil.getAddresses("localhost:11211") );
    testFramework = new KeyValueStoreOperatorTest<MemcacheStore>( operatorStore, testStore );
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
