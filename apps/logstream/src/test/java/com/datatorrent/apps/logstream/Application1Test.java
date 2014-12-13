/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.apps.logstream;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;
import org.junit.Ignore;

/**
 *
 * Tests the new logstream application.
 */
public class Application1Test
{
  @Test
  @Ignore
  public void testSomeMethod() throws Exception
  {
    Configuration conf = new Configuration(false);
    LocalMode lma = LocalMode.newInstance();

    Application1 application = new Application1();

    application.populateDAG(lma.getDAG(), conf);
    lma.cloneDAG();
    lma.getController().run(60000);
  }

}
