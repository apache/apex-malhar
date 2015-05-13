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

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.Result;

public class QueryProcessorTest
{
  @Test
  public void simpleTest()
  {
    final int numQueries = 3;

    QueryProcessor<Query, Void, Void, Void, Result> queryProcessor = QueryProcessor.newInstance(new SimpleQueryComputer());

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
