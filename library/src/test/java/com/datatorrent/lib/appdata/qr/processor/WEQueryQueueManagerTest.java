/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr.processor;

import com.datatorrent.lib.appdata.qr.Query;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class WEQueryQueueManagerTest
{
  @Test
  public void testSimpleRemoveEmpty()
  {
    WEQueryQueueManager<Query, Void> wqqm = new WEQueryQueueManager<Query, Void>();

    wqqm.setup(null);
    wqqm.beginWindow(0);

    QueryBundle<Query, Void> qb = wqqm.dequeue();
    Query queryD = qb == null ? null : qb.getQuery();
    Assert.assertEquals("The queries must match.", null, queryD);

    qb = wqqm.dequeue();
    queryD = qb == null ? null : qb.getQuery();
    Assert.assertEquals("The queries must match.", null, queryD);

    wqqm.endWindow();
    wqqm.teardown();
  }

  @Test
  public void testSimpleAddOneRemove()
  {
    WEQueryQueueManager<Query, Void> wqqm = new WEQueryQueueManager<Query, Void>();

    wqqm.setup(null);
    wqqm.beginWindow(0);

    Query query = new Query();
    wqqm.enqueue(query, null, 1L);

    Query queryD = wqqm.dequeue().getQuery();
    QueryBundle<Query, Void> qb = wqqm.dequeue();
    Query queryD1 = qb == null ? null : qb.getQuery();

    wqqm.endWindow();
    wqqm.teardown();

    Assert.assertEquals("The queries must match.", query, queryD);
    Assert.assertEquals("The queries must match.", null, queryD1);
  }

  @Test
  public void testSimpleAddRemove2()
  {
    WEQueryQueueManager<Query, Void> wqqm = new WEQueryQueueManager<Query, Void>();

    wqqm.setup(null);
    wqqm.beginWindow(0);

    Query query = new Query();
    wqqm.enqueue(query, null, 1L);

    Query queryD = wqqm.dequeue().getQuery();
    QueryBundle<Query, Void> qb = wqqm.dequeue();
    Query queryD1 = qb == null ? null : qb.getQuery();

    Query query1 = new Query();
    wqqm.enqueue(query1, null, 1L);

    Query query1D = wqqm.dequeue().getQuery();
    qb = wqqm.dequeue();
    Query query1D1 = qb == null ? null : qb.getQuery();

    wqqm.endWindow();
    wqqm.teardown();

    Assert.assertEquals("The queries must match.", query, queryD);
    Assert.assertEquals("The queries must match.", null, queryD1);

    Assert.assertEquals("The queries must match.", query1, query1D);
    Assert.assertEquals("The queries must match.", null, query1D1);
  }

  @Test
  public void testSimpleAddAfterStarted()
  {
    WEQueryQueueManager<Query, Void> wqqm = new WEQueryQueueManager<Query, Void>();

    wqqm.setup(null);
    wqqm.beginWindow(0);

    Query query = new Query();
    query.setId("0");
    wqqm.enqueue(query, null, 1L);

    Query query1 = new Query();
    query1.setId("1");
    wqqm.enqueue(query1, null, 1L);

    Query queryD = wqqm.dequeue().getQuery();

    Query query2 = new Query();
    query2.setId("2");
    wqqm.enqueue(query2, null, 1L);

    Query query1D = wqqm.dequeue().getQuery();
    Query query2D = wqqm.dequeue().getQuery();
    QueryBundle<Query, Void> qb = wqqm.dequeue();
    Query query3D = qb == null ? null : qb.getQuery();

    wqqm.endWindow();
    wqqm.teardown();

    Assert.assertEquals("The queries must match.", query, queryD);
    Assert.assertEquals("The queries must match.", query1, query1D);
    Assert.assertEquals("The queries must match.", query2, query2D);
    Assert.assertEquals("The queries must match.", null, query3D);
  }

  @Test
  public void testResetRead()
  {
    final int numQueries = 3;

    WEQueryQueueManager<Query, Void> wqqm = new WEQueryQueueManager<Query, Void>();

    wqqm.setup(null);
    wqqm.beginWindow(0);

    for(int qc = 0;
        qc < numQueries;
        qc++) {
      Query query = new Query();
      query.setId(Integer.toString(qc));
      wqqm.enqueue(query, null, 3L);
    }

    Query query = wqqm.dequeue().getQuery();
    Query query1 = wqqm.dequeue().getQuery();

    Assert.assertEquals("Query ids must equal.", "0", query.getId());
    Assert.assertEquals("Query ids must equal.", "1", query1.getId());

    wqqm.endWindow();
    wqqm.beginWindow(1);

    {
      int qc = 0;

      for(QueryBundle<Query, Void> tquery;
          (tquery = wqqm.dequeue()) != null;
          qc++) {
        Assert.assertEquals("Query ids must equal.",
                            Integer.toString(qc),
                            tquery.getQuery().getId());
      }

      Assert.assertEquals("The number of queries must match.",
                          numQueries,
                          qc);
    }

    wqqm.endWindow();
    wqqm.teardown();
  }

  @Test
  public void testExpirationReadAll()
  {
    final int numQueries = 3;

    WEQueryQueueManager<Query, Void> wqqm = new WEQueryQueueManager<Query, Void>();

    wqqm.setup(null);
    wqqm.beginWindow(0);

    for(int qc = 0;
        qc < numQueries;
        qc++) {
      Query query = new Query();
      query.setId(Integer.toString(qc));
      wqqm.enqueue(query, null, 2L);
    }

    wqqm.endWindow();
    wqqm.beginWindow(1);

    {
      int qc = 0;

      for(QueryBundle<Query, Void> qb;
          (qb = wqqm.dequeue()) != null;
          qc++) {
        Query query = qb.getQuery();
        Assert.assertEquals("Query ids must equal.", Integer.toString(qc), query.getId());
      }

      Assert.assertEquals("The number of queries must match.", numQueries, qc);
    }

    wqqm.endWindow();
    wqqm.beginWindow(2);

    Assert.assertEquals("There should be no queries now", null, wqqm.dequeue());

    wqqm.endWindow();
    wqqm.teardown();
  }

  @Test
  public void testMixedExpiration()
  {
    final int numQueries = 3;
    WEQueryQueueManager<Query, Void> wqqm = new WEQueryQueueManager<Query, Void>();

    wqqm.setup(null);
    wqqm.beginWindow(0);

    {
      for(int qc = 0;
          qc < numQueries;
          qc++) {
        Query query = new Query();
        query.setId(Integer.toString(qc));
        wqqm.enqueue(query, null, 2L);
      }

      for(int qc = 0;
          qc < numQueries;
          qc++) {
        Query query = new Query();
        query.setId(Integer.toString(qc + numQueries));
        wqqm.enqueue(query, null, 3L);
      }
    }

    wqqm.endWindow();
    wqqm.beginWindow(1);

    {
      int qc = 0;

      for(QueryBundle<Query, Void> qb;
          (qb = wqqm.dequeue()) != null;
          qc++) {
        Query query = qb.getQuery();
        Assert.assertEquals("Query ids must equal.", Integer.toString(qc), query.getId());
      }

      Assert.assertEquals("The number of queries must match.", 2 * numQueries, qc);
    }

    wqqm.endWindow();
    wqqm.beginWindow(2);

    {
      int qc = 0;

      for(QueryBundle<Query, Void> qb;
          (qb = wqqm.dequeue()) != null;
          qc++) {
        Query query = qb.getQuery();
        Assert.assertEquals("Query ids must equal.", Integer.toString(qc + numQueries), query.getId());
      }

      Assert.assertEquals("The number of queries must match.", numQueries, qc);
    }

    wqqm.endWindow();
    wqqm.beginWindow(3);

    Assert.assertEquals("There should be no queries now", null, wqqm.dequeue());

    wqqm.endWindow();
    wqqm.teardown();
  }
}
