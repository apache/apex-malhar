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

package com.datatorrent.demos.oldfaithful;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.LocalMode;

public class OldFaithfulApplicationTest
{
  
  private static final Logger LOG = LoggerFactory.getLogger(OldFaithfulApplicationTest.class);

  @Test
  public void testSomeMethod() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    OldFaithfulApplication app = new OldFaithfulApplication();
    app.populateDAG(lma.getDAG(), new Configuration(false));

    try {
      LocalMode.Controller lc = lma.getController();
      lc.setHeartbeatMonitoringEnabled(false);
      lc.run(5000);
    } catch (Exception e) {
      LOG.error("Exception: ", e);
      Assert.fail("Unexpected exception.");
    }
  }
}
