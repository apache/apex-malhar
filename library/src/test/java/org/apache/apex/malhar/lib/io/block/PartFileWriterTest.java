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
package org.apache.apex.malhar.lib.io.block;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.apex.malhar.lib.io.fs.AbstractFileSplitter;
import org.apache.commons.io.FileUtils;
import com.google.common.collect.Maps;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.netlet.util.Slice;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

public class PartFileWriterTest
{

  public static final int BLOCK_SIZE = 5;
  public static String FILE_NAME = "FILE";

  public static final String[] FILE_CONTENTS = {"abcdefgh", "pqrst", "xyz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
    "0123456789" };

  private class TestMeta extends TestWatcher
  {
    String outputPath;
    //Maintain the sequence of records and process one by one through the input port of PartFileWriter.
    List<List<AbstractBlockReader.ReaderRecord<Slice>>> blockDataList = new ArrayList<>(FILE_CONTENTS.length);
    Map<Long, String> blockIdToExpectedContent = Maps.newHashMap();

    PartFileWriter underTest;
    File blocksDir;
    Context.OperatorContext context;
    List<AbstractFileSplitter.FileMetadata> fileMetadatas = new ArrayList<>();

    /* (non-Javadoc)
     * @see org.junit.rules.TestWatcher#starting(org.junit.runner.Description)
     */
    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      outputPath = new File("target/" + description.getClassName() + "/" + description.getMethodName()).getPath();

      underTest = new PartFileWriter();
      underTest.setOutputDirectoryPath(outputPath);

      try {
        File outDir = new File(outputPath);
        FileUtils.forceMkdir(outDir);

        blocksDir = new File(outputPath);
        blocksDir.mkdirs();

        populateBlocks();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    protected void populateBlocks()
    {
      long blockId = 1000;
      for (int i = 0; i < FILE_CONTENTS.length; i++) {
        int blockIndex = 0;
        List<AbstractBlockReader.ReaderRecord<Slice>> blockList = new ArrayList<>();
        AbstractFileSplitter.FileMetadata fileMetadata = new AbstractFileSplitter.FileMetadata(outputPath);
        fileMetadata.setRelativePath(FILE_NAME + i);
        String fileContents = FILE_CONTENTS[i];
        int fileLength = fileContents.length();
        int noOfBlocks = ((fileLength / BLOCK_SIZE) + (((fileLength % BLOCK_SIZE) == 0) ? 0 : 1));
        long[] blockIds = new long[noOfBlocks];
        for (int offset = 0; offset < fileLength; offset += BLOCK_SIZE, blockId++) {
          String blockContents;
          if (offset + BLOCK_SIZE < fileLength) {
            blockContents = fileContents.substring(offset, offset + BLOCK_SIZE);
          } else {
            blockContents = fileContents.substring(offset);
          }

          AbstractBlockReader.ReaderRecord<Slice> readerRecord = new AbstractBlockReader.ReaderRecord<Slice>(blockId, new Slice(blockContents.getBytes()));
          blockIds[blockIndex] = blockId;
          blockIndex++;
          blockIdToExpectedContent.put(blockId, blockContents);
          blockList.add(readerRecord);
        }
        blockDataList.add(blockList);
        fileMetadata.setBlockIds(blockIds);
        fileMetadata.setNumberOfBlocks(noOfBlocks);
        fileMetadatas.add(fileMetadata);
      }
    }

    /* (non-Javadoc)
     * @see org.junit.rules.TestWatcher#finished(org.junit.runner.Description)
     */
    @Override
    protected void finished(Description description)
    {
      super.finished(description);

      try {
        FileUtils.deleteDirectory(new File(outputPath));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public PartFileWriterTest.TestMeta testMeta = new PartFileWriterTest.TestMeta();

  @Test
  public void testBlockWriting() throws IOException
  {
    Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.DAGContext.APPLICATION_ID, "PartitionWriterTest");
    attributes.put(DAG.DAGContext.APPLICATION_PATH, testMeta.outputPath);
    testMeta.context = mockOperatorContext(1, attributes);

    testMeta.underTest.setup(testMeta.context);
    for (int fileIndex = 0; fileIndex < FILE_CONTENTS.length; fileIndex++) {
      testMeta.underTest.beginWindow(fileIndex);
      testMeta.underTest.fileMetadataInput.process(testMeta.fileMetadatas.get(fileIndex));
      for (int blockIndex = 0; blockIndex < testMeta.fileMetadatas.get(fileIndex).getNumberOfBlocks(); blockIndex++) {
        testMeta.underTest.input.process(testMeta.blockDataList.get(fileIndex).get(blockIndex));
      }
      testMeta.underTest.endWindow();
    }
    testMeta.underTest.committed(2);
    testMeta.underTest.teardown();
    File[] blockFileNames = testMeta.blocksDir.listFiles();
    for (File blockFile : blockFileNames) {
      int fileIndex = Integer.parseInt(blockFile.getName().split("\\.")[0].replaceAll(FILE_NAME, ""));
      //Ignore the PartFileWriter.PARTSUFFIX and trailing "."
      int blockIndex = Integer.parseInt(blockFile.getName().split("\\.")[1].replaceAll(PartFileWriter.PARTSUFFIX.substring(1), ""));
      String expected = testMeta.blockIdToExpectedContent.get(testMeta.fileMetadatas.get(fileIndex).getBlockIds()[blockIndex - 1]);
      Assert.assertEquals(expected, FileUtils.readFileToString(blockFile));
    }
  }
}
