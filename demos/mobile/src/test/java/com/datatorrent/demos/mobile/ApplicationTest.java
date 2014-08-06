/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.demos.mobile;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.datatorrent.api.LocalMode;
import com.datatorrent.demos.mobile.Application;


public class ApplicationTest
{
  public ApplicationTest()
  {
  }

  /**
   * Test of getApplication method, of class Application.
   */
  @Test
  public void testGetApplication() throws Exception
  {
	  Application app = new Application();
		Configuration conf =new Configuration(false);
		conf.addResource("dt-site-mobile.xml");
	    LocalMode lma = LocalMode.newInstance();
	    lma.prepareDAG(app, conf);
	    LocalMode.Controller lc = lma.getController();
	    lc.run(10000);
   
  }
}
