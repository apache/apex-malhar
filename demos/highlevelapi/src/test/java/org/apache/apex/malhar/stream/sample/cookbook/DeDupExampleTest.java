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
package org.apache.apex.malhar.stream.sample.cookbook;

import java.util.concurrent.Callable;

import org.junit.Assert;
import org.junit.Test;
import org.apache.hadoop.conf.Configuration;
import com.datatorrent.api.LocalMode;

import com.datatorrent.stram.StramLocalCluster;


/**
 * Test for {@link DeDupExample}.
 */
public class DeDupExampleTest
{
  @Test
  public void DeDupExampleTest() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    conf.set("dt.application.DeDupExample.operator.console.silent", "true");
    DeDupExample app = new DeDupExample();
    lma.prepareDAG(app, conf);
    LocalMode.Controller lc = lma.getController();
    ((StramLocalCluster)lc).setExitCondition(new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {
        return DeDupExample.Collector.isDone();
      }
    });
    lc.run(50000);

    Assert.assertEquals(9, DeDupExample.Collector.getResult().getValue().size());

  }

}
