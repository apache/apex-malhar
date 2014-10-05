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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.testbench.CollectorTestSink;

import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;

/**
 * Functional test for {
 *
 * @linkcom.datatorrent.lib.io.FtpInputOperator .
 */
public class FtpInputOperatorTest
{
  private FakeFtpServer fakeFtpServer;
  private static final String HOME_DIR = "/";
  private static final String FILE = "/dir/sample.txt";
  private static final String CONTENTS = "abcdef 1234567890";
  private int port;
  private FtpInputOperator ftpInputOperator;

  @Before
  public void setup() throws Exception
  {
    fakeFtpServer = new FakeFtpServer();
    fakeFtpServer.setServerControlPort(0);
    FileSystem fileSystem = new UnixFakeFileSystem();
    fileSystem.add(new FileEntry(FILE, CONTENTS));
    fakeFtpServer.setFileSystem(fileSystem);
    fakeFtpServer.addUserAccount(new UserAccount("testUser", "password", HOME_DIR));
    fakeFtpServer.start();
    port = fakeFtpServer.getServerControlPort();
    ftpInputOperator = new FtpInputOperator();
  }

  @After
  public void teardown() throws Exception
  {
    if (fakeFtpServer != null) {
      fakeFtpServer.stop();
    }
    ftpInputOperator.teardown();
  }

  @Test
  public void TestFtpInputOperator()
  {
    ftpInputOperator.setFtpServer("localhost");
    ftpInputOperator.setPort(port);
    ftpInputOperator.setFilePath(FILE);
    ftpInputOperator.setLocalPassiveMode(true);
    ftpInputOperator.setNumberOfTuples(10);
    ftpInputOperator.setUserName("testUser");
    ftpInputOperator.setPassword("password");
    ftpInputOperator.setDelay(1);
    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    ftpInputOperator.output.setSink(sink);
    ftpInputOperator.setup(null);
    ftpInputOperator.beginWindow(0);
    ftpInputOperator.emitTuples();
    ftpInputOperator.endWindow();
    Assert.assertEquals(1, sink.collectedTuples.size());
    for (int i = 0; i < sink.collectedTuples.size(); i++) {
      Assert.assertEquals(CONTENTS, sink.collectedTuples.get(i));
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(FtpInputOperatorTest.class);
}
