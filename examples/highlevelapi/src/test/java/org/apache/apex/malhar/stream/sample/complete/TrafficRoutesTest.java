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

import java.util.Map;
import java.util.concurrent.Callable;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;
import com.datatorrent.stram.StramLocalCluster;

/**
 * Testing the {@link TrafficRoutes} Application.
 */
public class TrafficRoutesTest
{

  @Test
  public void TrafficRoutesTest() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    conf.set("dt.application.TrafficRoutes.operator.console.silent", "true");
    lma.prepareDAG(new TrafficRoutes(), conf);
    LocalMode.Controller lc = lma.getController();

    ((StramLocalCluster)lc).setExitCondition(new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {
        return TrafficRoutes.InfoGen.getTupleCount() >= 100;
      }
    });

    lc.run(60000);

    Assert.assertTrue(!TrafficRoutes.Collector.getResult().isEmpty());
    for (Map.Entry<KeyValPair<Long, String>, KeyValPair<Double, Boolean>> entry : TrafficRoutes.Collector.getResult().entrySet()) {
      Assert.assertTrue(entry.getValue().getKey() <= 75);
      Assert.assertTrue(entry.getValue().getKey() >= 55);
      Assert.assertTrue(entry.getKey().getValue().equals("SDRoute1") || entry.getKey().getValue().equals("SDRoute2"));
    }
  }

}
