/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr.processor;

import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.processor.QueryBundle;
import com.datatorrent.lib.appdata.qr.processor.SimpleDoneQueryQueueManager;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class SimpleDoneQueryQueueManagerTest
{
  @Test
  public void firstDoneTest()
  {
    SimpleDoneQueryQueueManager<Query, Void> sdqqm = new SimpleDoneQueryQueueManager<Query, Void>();

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
    SimpleDoneQueryQueueManager<Query, Void> sdqqm = new SimpleDoneQueryQueueManager<Query, Void>();

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
    SimpleDoneQueryQueueManager<Query, Void> sdqqm = new SimpleDoneQueryQueueManager<Query, Void>();

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
