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
package com.datatorrent.apps.logstream.PropertyRegistry;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Tests logstream property registry
 */
public class LogstreamPropertyRegistryTest
{
  @Test
  public void test()
  {
    LogstreamPropertyRegistry registry = new LogstreamPropertyRegistry();

    registry.bind("ONE", "a");
    registry.bind("ONE", "b");
    registry.bind("ONE", "c");

    registry.bind("TWO", "x");

    registry.bind("THREE", "1");
    registry.bind("THREE", "2");

    Assert.assertEquals("index 0", registry.getIndex("ONE", "a"), 0);

    Assert.assertEquals("index 0 name", "ONE", registry.lookupName(0));
    Assert.assertEquals("index 0 value", "a", registry.lookupValue(0));

    String[] list = registry.list("ONE");
    Assert.assertEquals("list size", 3, list.length);
    Assert.assertEquals("list value", "a", list[0]);
  }

}
