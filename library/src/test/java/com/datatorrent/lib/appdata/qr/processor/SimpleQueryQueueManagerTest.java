/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr.processor;

import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.processor.QueryBundle;
import com.datatorrent.lib.appdata.qr.processor.SimpleQueryQueueManager;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class SimpleQueryQueueManagerTest
{
  @Test
  public void simpleTest()
  {
    SimpleQueryQueueManager<Query, Void, Void> sqqm = new SimpleQueryQueueManager<Query, Void, Void>();

    sqqm.setup(null);
    sqqm.beginWindow(0);

    Query query = new Query();
    query.setId("1");

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
