/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.stream.sample.complete;

import java.util.List;
import java.util.concurrent.Callable;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;
import com.datatorrent.stram.StramLocalCluster;

/**
 * Testing the {@link TopWikipediaSessions} Application.
 */
public class TopWikipediaSessionsTest
{
  @Test
  public void TopWikipediaSessionsTest() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    conf.set("dt.application.TopWikipediaSessions.operator.console.silent", "true");
    lma.prepareDAG(new TopWikipediaSessions(), conf);
    LocalMode.Controller lc = lma.getController();

    ((StramLocalCluster)lc).setExitCondition(new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {
        return TopWikipediaSessions.SessionGen.getTupleCount() >= 250;
      }
    });

    lc.run(30000);

    for (int i = 0; i < TopWikipediaSessions.Collector.getResult().size(); i++) {
      Assert.assertTrue(isInOrder(TopWikipediaSessions.Collector.getResult().get(i)));
    }
  }

  public boolean isInOrder(List<TopWikipediaSessions.TempWrapper> input)
  {
    if (input.size() == 0 || input.size() == 1) {
      return true;
    }
    for (int i = 0; i < input.size() - 2; i++) {
      if (input.get(i).getValue().getValue() < input.get(i + 1).getValue().getValue()) {
        return false;
      }
    }
    return true;
  }
}
