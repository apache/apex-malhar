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

package org.apache.apex.malhar.lib.io.fs;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.apex.malhar.lib.io.block.BlockMetadata.FileBlockMetadata;
import org.apache.apex.malhar.lib.io.block.BlockWriter;
import org.apache.apex.malhar.lib.io.fs.AbstractFileSplitter.FileMetadata;
import org.apache.apex.malhar.lib.io.fs.Synchronizer.OutputFileMetadata;
import org.apache.apex.malhar.lib.io.fs.Synchronizer.StitchBlock;
import org.apache.apex.malhar.lib.io.fs.Synchronizer.StitchBlockMetaData;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

/**
 * Unit tests for {@link FileStitcher}
 */
public class FileStitcherTest
{
  public static final String[] FILE_CONTENTS = {"abcdefghi", "pqr", "hello world", "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
      "0123456789" };

  public static final int BLOCK_SIZE = 5;

  private class TestMeta extends TestWatcher
  {
    String outputPath;
    List<FileMetadata> fileMetadataList = Lists.newArrayList();

    FileStitcher<OutputFileMetadata> oper;
    File blocksDir;
    Context.OperatorContext context;

    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      outputPath = new File("target/" + description.getClassName() + "/" + description.getMethodName()).getPath();

      oper = new FileStitcher<OutputFileMetadata>();
      oper.setFilePath(outputPath);
      String appDirectory = outputPath;

      Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.DAGContext.APPLICATION_ID, description.getClassName());
      attributes.put(DAG.DAGContext.APPLICATION_PATH, appDirectory);
      context = mockOperatorContext(1, attributes);

      oper.setup(context);

      try {
        File outDir = new File(outputPath);
        FileUtils.forceMkdir(outDir);

        blocksDir = new File(context.getValue(Context.DAGContext.APPLICATION_PATH), BlockWriter.DEFAULT_BLOCKS_DIR);
        blocksDir.mkdirs();

        long blockID = 1000;

        for (int i = 0; i < FILE_CONTENTS.length; i++) {
          List<Long> blockIDs = Lists.newArrayList();

          File file = new File(outputPath, i + ".txt");

          FileUtils.write(file, FILE_CONTENTS[i]);

          int offset = 0;
          for (; offset < FILE_CONTENTS[i].length(); offset += BLOCK_SIZE, blockID++) {
            String blockContents;
            if (offset + BLOCK_SIZE < FILE_CONTENTS[i].length()) {
              blockContents = FILE_CONTENTS[i].substring(offset, offset + BLOCK_SIZE);
            } else {
              blockContents = FILE_CONTENTS[i].substring(offset);
            }
            FileUtils.write(new File(blocksDir, blockID + ""), blockContents);
            blockIDs.add(blockID);
          }

          FileMetadata fileMetadata = new FileMetadata(file.getPath());
          fileMetadata.setBlockIds(ArrayUtils.toPrimitive(blockIDs.toArray(new Long[0])));
          fileMetadataList.add(fileMetadata);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
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
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testFileMerger() throws IOException, InterruptedException
  {
    long[][][] partitionMeta = {
        {{1000, 0, 5 }, {1001, 0, 4 } }, {{1002, 0, 3 } }, //testing multiple blocks from same block file
        {{1003, 0, 5 }, {1004, 0, 5 }, {1005, 0, 1 } },
        {{1006, 0, 5 }, {1007, 0, 5 }, {1008, 0, 5 }, {1009, 0, 5 }, {1010, 0, 5 }, {1011, 0, 1 } },
        {{1012, 0, 5 }, {1013, 0, 5 } } };

    testMeta.oper.beginWindow(0);
    long fileID = 0;
    for (int tupleIndex = 0; tupleIndex < partitionMeta.length; tupleIndex++) {
      OutputFileMetadata ingestionFileMetaData = new OutputFileMetadata();
      String fileName = fileID++ + ".txt";
      ingestionFileMetaData.setRelativePath(fileName);
      List<StitchBlock> outputBlocks = Lists.newArrayList();
      for (long[] block : partitionMeta[tupleIndex]) {
        String blockFilePath = new Path(testMeta.outputPath, Long.toString(block[0])).toString();
        FileBlockMetadata fileBlockMetadata = new FileBlockMetadata(blockFilePath, block[0], block[1], block[2], false,
            -1);
        StitchBlockMetaData outputFileBlockMetaData = new StitchBlockMetaData(fileBlockMetadata, fileName, false);
        outputBlocks.add(outputFileBlockMetaData);
      }
      ingestionFileMetaData.setOutputBlockMetaDataList(outputBlocks);
      testMeta.oper.input.process(ingestionFileMetaData);
    }
    testMeta.oper.endWindow();
    testMeta.oper.committed(0);
    //give some time to complete postCommit operations
    Thread.sleep(2 * 1000);

    for (int fileId = 0; fileId < partitionMeta.length; fileId++) {
      String fromFile = FileUtils.readFileToString(new File(testMeta.oper.getFilePath(), fileId + ".txt"));
      Assert.assertEquals("File " + fileId + "not matching", FILE_CONTENTS[fileId], fromFile);
    }
  }

}
