/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.lib.appdata.qr.processor;

import com.datatorrent.lib.appdata.query.serde.Query;
import com.datatorrent.lib.appdata.query.QueryBundle;
import com.datatorrent.lib.appdata.query.SimpleDoneQueueManager;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Assert;
import org.junit.Test;

public class SimpleDoneQueryQueueManagerTest
{
  @Test
  public void firstDoneTest()
  {
    SimpleDoneQueueManager<Query, Void> sdqqm = new SimpleDoneQueueManager<Query, Void>();

    sdqqm.setup(null);
    sdqqm.beginWindow(0);

    Query query = new Query();
    sdqqm.enqueue(query, null, new MutableBoolean(true));

    QueryBundle<Query, Void, MutableBoolean> qb = sdqqm.dequeue();
    Assert.assertEquals("Should return back null.", null, qb);

    sdqqm.endWindow();
    sdqqm.beginWindow(1);

    qb = sdqqm.dequeue();
    Assert.assertEquals("Should return back null.", null, qb);
    qb = sdqqm.dequeue();
    Assert.assertEquals("Should return back null.", null, qb);

    sdqqm.endWindow();
    sdqqm.teardown();
  }

  @Test
  public void simpleEnqueueDequeue()
  {
    SimpleDoneQueueManager<Query, Void> sdqqm = new SimpleDoneQueueManager<Query, Void>();

    sdqqm.setup(null);
    sdqqm.beginWindow(0);

    Query query = new Query();
    sdqqm.enqueue(query, null, new MutableBoolean(false));

    QueryBundle<Query, Void, MutableBoolean> qb = sdqqm.dequeue();

    Assert.assertEquals("Should return same query.", query, qb.getQuery());
    qb = sdqqm.dequeue();
    Assert.assertEquals("Should return back null.", null, qb);

    sdqqm.endWindow();
    sdqqm.beginWindow(1);

    qb = sdqqm.dequeue();
    Assert.assertEquals("Should return same query.", query, qb.getQuery());
    qb = sdqqm.dequeue();
    Assert.assertEquals("Should return back null.", null, qb);

    sdqqm.endWindow();
    sdqqm.teardown();
  }

  @Test
  public void simpleExpire1()
  {
    SimpleDoneQueueManager<Query, Void> sdqqm = new SimpleDoneQueueManager<Query, Void>();

    sdqqm.setup(null);
    sdqqm.beginWindow(0);

    Query query = new Query();
    sdqqm.enqueue(query, null, new MutableBoolean(false));

    QueryBundle<Query, Void, MutableBoolean> qb = sdqqm.dequeue();

    Assert.assertEquals("Should return same query.", query, qb.getQuery());
    qb.getQueueContext().setValue(true);

    qb = sdqqm.dequeue();
    Assert.assertEquals("Should return back null.", null, qb);

    sdqqm.endWindow();
    sdqqm.beginWindow(1);

    qb = sdqqm.dequeue();
    Assert.assertEquals("Should return back null.", null, qb);

    sdqqm.endWindow();
    sdqqm.teardown();
  }
}
