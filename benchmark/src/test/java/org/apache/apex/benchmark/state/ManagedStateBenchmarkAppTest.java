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
package org.apache.apex.benchmark.state;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import org.apache.apex.benchmark.state.StoreOperator.ExecMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;

/**
 * This is not a really unit test, but in fact a benchmark runner.
 * Provides this class to give developers the convenience to run in local IDE environment.
 */
public class ManagedStateBenchmarkAppTest extends ManagedStateBenchmarkApp
{
  public static final String basePath = "target/temp";

  @Before
  public void before()
  {
    FileUtil.fullyDelete(new File(basePath));
  }

  @Test
  public void testUpdateSync() throws Exception
  {
    test(ExecMode.UPDATE_SYNC);
  }

  @Test
  public void testUpdateAsync() throws Exception
  {
    test(ExecMode.UPDATE_ASYNC);
  }

  @Test
  public void testInsert() throws Exception
  {
    test(ExecMode.INSERT);
  }

  public void test(ExecMode exeMode) throws Exception
  {
    Configuration conf = new Configuration(false);

    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    super.populateDAG(dag, conf);
    storeOperator.setExecMode(exeMode);

    StreamingApplication app = new StreamingApplication()
    {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
      }
    };

    lma.prepareDAG(app, conf);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.run(3000000);

    lc.shutdown();
  }

  @Override
  public String getStoreBasePath(Configuration conf)
  {
    return basePath;
  }
}
