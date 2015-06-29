/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.uniquecount;

import com.datatorrent.api.LocalMode;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Test the DAG declaration in local mode.
 */
public class UniqueKeyValDemoTest
{
  @Test
  public void testApplication() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    new UniqueKeyValCountDemo().populateDAG(lma.getDAG(), new Configuration(false));
    LocalMode.Controller lc = lma.getController();
    lc.run(10000);
  }
}
