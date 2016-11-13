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
package org.apache.apex.malhar.lib.utils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;

public class IOUtilsTest
{

  @Test
  public void testCopyPartial() throws IOException
  {
    testCopyPartialHelper(1024, 0, 512);
  }

  @Test
  public void testCopyPartialLarge() throws IOException
  {
    testCopyPartialHelper(2048, 0, 1068);
  }

  @Test
  public void testCopyFromOffset() throws IOException
  {
    testCopyPartialHelper(2048, 1024, 1024);
  }

  private void testCopyPartialHelper(int dataSize, int offset, long size) throws IOException
  {
    FileUtils.deleteQuietly(new File("target/IOUtilsTest"));
    File file = new File("target/IOUtilsTest/testCopyPartial/input");
    createDataFile(file, dataSize);

    FileContext fileContext = FileContext.getFileContext();
    DataInputStream inputStream = fileContext.open(new Path(file.getAbsolutePath()));

    Path output = new Path("target/IOUtilsTest/testCopyPartial/output");
    DataOutputStream outputStream = fileContext.create(output, EnumSet
        .of(CreateFlag.CREATE, CreateFlag.OVERWRITE), Options.CreateOpts.CreateParent.createParent());

    if (offset == 0) {
      IOUtils.copyPartial(inputStream, size, outputStream);
    } else {
      IOUtils.copyPartial(inputStream, offset, size, outputStream);
    }

    outputStream.close();

    Assert.assertTrue("output exists", fileContext.util().exists(output));
    Assert.assertEquals("output size", size, fileContext.getFileStatus(output).getLen());
//    FileUtils.deleteQuietly(new File("target/IOUtilsTest"));
  }

  private void createDataFile(File file, int dataSize) throws IOException
  {
    byte[] val = new byte[dataSize];
    Random random = new Random();
    random.nextBytes(val);

    FileUtils.write(file, new String(val));
  }
}
