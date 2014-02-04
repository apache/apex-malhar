/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.memcache_whalin;

import com.datatorrent.lib.db.KeyValueStoreOperatorTest;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @since 0.9.3
 */
public class MemcacheWhalinOperatorTest
{
  MemcacheStore store;
  KeyValueStoreOperatorTest<MemcacheStore> testFramework;

  @Before
  public void setup()
  {
    store = new MemcacheStore();
    testFramework = new KeyValueStoreOperatorTest<MemcacheStore>(store);
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
