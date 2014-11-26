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
package com.datatorrent.lib.io;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;

import com.datatorrent.lib.io.AbstractFTPDirectoryInputOperator.FTPDirectoryStringInputOperator;
import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 * Tests for {@link FTPDirectoryStringInputOperator}
 */
public class FTPDirectoryStringInputOperatorTest
{
  public static class TestMeta extends TestWatcher
  {
    String baseDir;
    String ftpDir;
    FTPDirectoryStringInputOperator ftpOperator;
    FakeFtpServer fakeFtpServer;
    CollectorTestSink<Object> sink;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      String methodName = description.getMethodName();
      String className = description.getClassName();
      baseDir = "target/" + className + "/" + methodName;
      ftpDir = new File(this.baseDir + "/ftp").getAbsolutePath();

      fakeFtpServer = new FakeFtpServer();
      fakeFtpServer.setServerControlPort(0);
      fakeFtpServer.addUserAccount(new UserAccount("testUser", "test", ftpDir));

      UnixFakeFileSystem fileSystem = new UnixFakeFileSystem();
      fileSystem.add(new FileEntry(ftpDir + "/1.txt", "1\n10\n"));
      fileSystem.add(new FileEntry(ftpDir + "/2.txt", "2\n20\n"));

      fakeFtpServer.setFileSystem(fileSystem);
      fakeFtpServer.start();

      ftpOperator = new FTPDirectoryStringInputOperator();
      ftpOperator.setHost("localhost");
      ftpOperator.setPort(fakeFtpServer.getServerControlPort());
      ftpOperator.setUserName("testUser");
      ftpOperator.setPassword("test");

      ftpOperator.setDirectory(ftpDir);
      ftpOperator.setup(null);

      sink = new CollectorTestSink<Object>();
      ftpOperator.output.setSink(sink);
    }

    @Override
    protected void finished(Description description)
    {
      try {
        ftpOperator.teardown();
        fakeFtpServer.stop();
        FileUtils.deleteDirectory(new File(baseDir));
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testFtpDirectoryInput()
  {
    testMeta.ftpOperator.beginWindow(0);
    for (int i = 0; i < 3; i++) {
      testMeta.ftpOperator.emitTuples();
    }
    testMeta.ftpOperator.endWindow();
    Assert.assertEquals("lines", 4, testMeta.sink.collectedTuples.size());
    Assert.assertTrue("1", testMeta.sink.collectedTuples.contains("1"));
    Assert.assertTrue("10", testMeta.sink.collectedTuples.contains("10"));
    Assert.assertTrue("2", testMeta.sink.collectedTuples.contains("2"));
    Assert.assertTrue("20", testMeta.sink.collectedTuples.contains("20"));
  }
}