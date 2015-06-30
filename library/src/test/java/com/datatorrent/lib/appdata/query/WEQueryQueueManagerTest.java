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

import org.apache.commons.lang3.mutable.MutableLong;

import com.datatorrent.lib.appdata.schemas.Query;

public class WEQueryQueueManagerTest
{
  @Test
  public void testSimpleRemoveEmpty()
  {
    WindowEndQueueManager<Query, Void> wqqm = new WindowEndQueueManager<Query, Void>();

    wqqm.setup(null);
    wqqm.beginWindow(0);

    QueryBundle<Query, Void, MutableLong> qb = wqqm.dequeue();
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
    WindowEndQueueManager<Query, Void> wqqm = new WindowEndQueueManager<Query, Void>();

    wqqm.setup(null);
    wqqm.beginWindow(0);

    Query query = new MockQuery("1");
    wqqm.enqueue(query, null, new MutableLong(1L));

    Query queryD = wqqm.dequeue().getQuery();
    QueryBundle<Query, Void, MutableLong> qb = wqqm.dequeue();
    Query queryD1 = qb == null ? null : qb.getQuery();

    wqqm.endWindow();
    wqqm.teardown();

    Assert.assertEquals("The queries must match.", query, queryD);
    Assert.assertEquals("The queries must match.", null, queryD1);
  }

  @Test
  public void testSimpleAddRemove2()
  {
    WindowEndQueueManager<Query, Void> wqqm = new WindowEndQueueManager<Query, Void>();

    wqqm.setup(null);
    wqqm.beginWindow(0);

    Query query = new MockQuery("1");
    wqqm.enqueue(query, null, new MutableLong(1L));

    Query queryD = wqqm.dequeue().getQuery();
    QueryBundle<Query, Void, MutableLong> qb = wqqm.dequeue();
    Query queryD1 = qb == null ? null : qb.getQuery();

    Query query1 = new MockQuery("2");
    wqqm.enqueue(query1, null, new MutableLong(1L));

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
    WindowEndQueueManager<Query, Void> wqqm = new WindowEndQueueManager<Query, Void>();

    wqqm.setup(null);
    wqqm.beginWindow(0);

    Query query = new MockQuery("0");
    wqqm.enqueue(query, null, new MutableLong(1L));

    Query query1 = new MockQuery("1");
    wqqm.enqueue(query1, null, new MutableLong(1L));

    Query queryD = wqqm.dequeue().getQuery();

    Query query2 = new MockQuery("2");
    wqqm.enqueue(query2, null, new MutableLong(1L));

    Query query1D = wqqm.dequeue().getQuery();
    Query query2D = wqqm.dequeue().getQuery();
    QueryBundle<Query, Void, MutableLong> qb = wqqm.dequeue();
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

    WindowEndQueueManager<Query, Void> wqqm = new WindowEndQueueManager<Query, Void>();

    wqqm.setup(null);
    wqqm.beginWindow(0);

    for(int qc = 0;
        qc < numQueries;
        qc++) {
      Query query = new MockQuery(Integer.toString(qc));
      wqqm.enqueue(query, null, new MutableLong(3L));
    }

    Query query = wqqm.dequeue().getQuery();
    Query query1 = wqqm.dequeue().getQuery();

    Assert.assertEquals("Query ids must equal.", "0", query.getId());
    Assert.assertEquals("Query ids must equal.", "1", query1.getId());

    wqqm.endWindow();
    wqqm.beginWindow(1);

    {
      int qc = 0;

      for(QueryBundle<Query, Void, MutableLong> tquery;
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

    WindowEndQueueManager<Query, Void> wqqm = new WindowEndQueueManager<Query, Void>();

    wqqm.setup(null);
    wqqm.beginWindow(0);

    for(int qc = 0;
        qc < numQueries;
        qc++) {
      Query query = new MockQuery(Integer.toString(qc));
      wqqm.enqueue(query, null, new MutableLong(2L));
    }

    wqqm.endWindow();
    wqqm.beginWindow(1);

    {
      int qc = 0;

      for(QueryBundle<Query, Void, MutableLong> qb;
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
    WindowEndQueueManager<Query, Void> wqqm = new WindowEndQueueManager<Query, Void>();

    wqqm.setup(null);
    wqqm.beginWindow(0);

    {
      for(int qc = 0;
          qc < numQueries;
          qc++) {
        Query query = new MockQuery(Integer.toString(qc));
        wqqm.enqueue(query, null, new MutableLong(2L));
      }

      for(int qc = 0;
          qc < numQueries;
          qc++) {
        Query query = new MockQuery(Integer.toString(qc + numQueries));
        wqqm.enqueue(query, null, new MutableLong(3L));
      }
    }

    wqqm.endWindow();
    wqqm.beginWindow(1);

    {
      int qc = 0;

      for(QueryBundle<Query, Void, MutableLong> qb;
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

      for(QueryBundle<Query, Void, MutableLong> qb;
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
