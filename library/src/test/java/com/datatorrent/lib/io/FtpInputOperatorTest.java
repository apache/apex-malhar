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
package com.datatorrent.lib.io;

import junit.framework.Assert;

import org.junit.Test;

import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 Functional test for {
 *
 * @linkcom.datatorrent.lib.io.FtpInputOperator }.
 *
 */
public class FtpInputOperatorTest
{
  @Test
  public void TestFtpInputOperator(){
    FtpInputOperator oper = new FtpInputOperator();
    oper.setFtpServer("ita.ee.lbl.gov");
    oper.setFilePath("/traces/NASA_access_log_Jul95.gz");
    oper.setLocalPassiveMode(true);
    oper.setNumberOfTuples(10);
    oper.setDelay(1);
    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    oper.output.setSink(sink);
    oper.setup(null);
    oper.beginWindow(0);
    oper.emitTuples();
    oper.endWindow();
    oper.teardown();
    Assert.assertEquals(oper.getNumberOfTuples(), sink.collectedTuples.size());    
  }
}
