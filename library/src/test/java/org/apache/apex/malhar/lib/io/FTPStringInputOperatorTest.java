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
package org.apache.apex.malhar.lib.io;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.DirectoryEntry;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;

import org.apache.apex.malhar.lib.io.AbstractFTPInputOperator.FTPStringInputOperator;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

import com.datatorrent.api.Attribute;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

/**
 * Tests for {@link FTPStringInputOperator}
 */
public class FTPStringInputOperatorTest
{
  public static class TestMeta extends TestWatcher
  {
    FTPStringInputOperator ftpOperator;
    FakeFtpServer fakeFtpServer;
    CollectorTestSink<Object> sink;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      UnixFakeFileSystem fileSystem = new UnixFakeFileSystem();
      DirectoryEntry homeDirectory = new DirectoryEntry("/home/test");
      fileSystem.add(homeDirectory);
      fileSystem.add(new FileEntry(homeDirectory.getPath() + "/1.txt", "1\n10\n"));
      fileSystem.add(new FileEntry(homeDirectory.getPath() + "/2.txt", "2\n20\n"));

      fakeFtpServer = new FakeFtpServer();
      fakeFtpServer.setServerControlPort(0);
      fakeFtpServer.addUserAccount(new UserAccount("testUser", "test", homeDirectory.getPath()));


      fakeFtpServer.setFileSystem(fileSystem);
      fakeFtpServer.start();

      ftpOperator = new FTPStringInputOperator();
      ftpOperator.setHost("localhost");
      ftpOperator.setPort(fakeFtpServer.getServerControlPort());
      ftpOperator.setUserName("testUser");
      ftpOperator.setPassword("test");

      ftpOperator.setDirectory(homeDirectory.getPath());
      ftpOperator.setup(mockOperatorContext(11, new Attribute.AttributeMap.DefaultAttributeMap()));

      sink = new CollectorTestSink<>();
      ftpOperator.output.setSink(sink);
    }

    @Override
    protected void finished(Description description)
    {
      ftpOperator.teardown();
      fakeFtpServer.stop();
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
