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
package com.datatorrent.lib.appdata.query;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.appdata.schemas.Query;

public class SimpleQueryQueueManagerTest
{
  @Test
  public void simpleTest()
  {
    SimpleQueueManager<Query, Void, Void> sqqm = new SimpleQueueManager<Query, Void, Void>();

    sqqm.setup(null);
    sqqm.beginWindow(0);

    Query query = new MockQuery("1");

    sqqm.enqueue(query, null, null);
    Query queryD = sqqm.dequeue().getQuery();
    QueryBundle<Query, Void, Void> qb = sqqm.dequeue();
    Query queryD1 = qb == null ? null : qb.getQuery();

    sqqm.teardown();
    sqqm.teardown();

    Assert.assertEquals("The query object must equal", query, queryD);
    Assert.assertEquals("The query object must equal", null, queryD1);
  }
}
