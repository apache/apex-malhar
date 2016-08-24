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

import java.util.concurrent.Callable;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;
import com.datatorrent.stram.StramLocalCluster;

/**
 * Testing the AutoComplete Application
 */
public class AutoCompleteTest
{

  @Test
  public void AutoCompleteTest() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    lma.prepareDAG(new AutoComplete(), conf);
    LocalMode.Controller lc = lma.getController();
    
    ((StramLocalCluster)lc).setExitCondition(new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {
        return AutoComplete.TweetsInput.isDone();
      }
    });
    
    lc.run(200000);
  
    Assert.assertTrue(AutoComplete.Collector.getResult().containsKey("chi"));
    Assert.assertTrue(AutoComplete.Collector.getResult().containsKey("china"));
    Assert.assertEquals(2, AutoComplete.Collector.getResult().get("china").get(0).getCount());
    Assert.assertEquals("China", AutoComplete.Collector.getResult().get("china").get(0).getValue());
    Assert.assertEquals(2, AutoComplete.Collector.getResult().get("d").size());
    Assert.assertEquals(3, AutoComplete.Collector.getResult().get("f").size());
    Assert.assertTrue(AutoComplete.Collector.getResult().get("f").get(0).getCount() >= AutoComplete.Collector.getResult().get("f").get(1).getCount());
    Assert.assertTrue(AutoComplete.Collector.getResult().get("f").get(1).getCount() >= AutoComplete.Collector.getResult().get("f").get(2).getCount());
  
  }
}
