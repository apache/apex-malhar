/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr.processor;

import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.Result;
import com.datatorrent.lib.appdata.qr.processor.QueryComputer;
import com.datatorrent.lib.appdata.qr.processor.QueryProcessor;
import com.google.common.collect.Lists;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class QueryProcessorTest
{
  @Test
  public void simpleTest()
  {
    final int numQueries = 3;

    QueryProcessor<Query, Void, Void, Void, Result> queryProcessor = new QueryProcessor<Query, Void, Void, Void, Result>(new SimpleQueryComputer());

    queryProcessor.setup(null);
    queryProcessor.beginWindow(0);

    for(int qc = 0;
        qc < numQueries;
        qc++) {
      Query query = new Query();
      query.setId(Integer.toString(qc));
      queryProcessor.enqueue(query, null, null);
    }

    Result result;
    List<Result> results = Lists.newArrayList();

    while((result = queryProcessor.process(null)) != null) {
      results.add(result);
    }

    queryProcessor.endWindow();
    queryProcessor.teardown();

    Assert.assertEquals("Sizes must match.", numQueries, results.size());

    for(int rc = 0;
        rc < results.size();
        rc++) {
      result = results.get(rc);
      Assert.assertEquals("Ids must match.", Integer.toString(rc), result.getId());
    }
  }

  public static class SimpleQueryComputer implements QueryComputer<Query, Void, Void, Void, Result>
  {
    public SimpleQueryComputer()
    {
    }

    @Override
    public void queueDepleted(Void context)
    {
    }

    @Override
    public Result processQuery(Query query, Void metaQuery, Void queueContext, Void context)
    {
      return new Result(query);
    }
  }
}
