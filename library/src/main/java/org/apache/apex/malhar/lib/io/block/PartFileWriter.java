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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.io.fs.AbstractFileSplitter;
import org.apache.commons.lang3.tuple.MutablePair;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.netlet.util.Slice;

/**
 * Writes the blocks into the specified directory.
 * If f1 is the file of size 10 MB and the block size is 1 MB then this operator writes the blocks into the
 * specified directory as f1.part1, f1.part2 , ...., f1.part10. Here, size of each part is 1 MB.
 *
 * @since 3.8.0
 */
public class PartFileWriter extends BlockWriter implements Operator.IdleTimeHandler
{
  protected static String PARTSUFFIX = ".part";
  @NotNull
  private String outputDirectoryPath;
  private Map<Long, MutablePair<Integer, String>> blockInfo = new HashMap<>();
  private transient List<AbstractBlockReader.ReaderRecord<Slice>> waitingTuples;

  public final transient DefaultInputPort<AbstractFileSplitter.FileMetadata> fileMetadataInput = new DefaultInputPort<AbstractFileSplitter.FileMetadata>()
  {
    @Override
    public void process(AbstractFileSplitter.FileMetadata fileMetadata)
    {
      blockInfo.clear();
      long[] blocks = fileMetadata.getBlockIds();
      String relativePath = fileMetadata.getRelativePath();
      for (int i = 0; i < blocks.length; i++) {
        blockInfo.put(blocks[i], new MutablePair<>(i + 1, relativePath));
      }
    }
  };

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    filePath = outputDirectoryPath;
    waitingTuples = new LinkedList<>();
  }

  @Override
  protected void processTuple(AbstractBlockReader.ReaderRecord<Slice> tuple)
  {
    // Check whether the fileMetadata of this blockid is received from fileMetadataInput port. If not, put it in waitingTuples.
    if (blockInfo.get(tuple.getBlockId()) == null) {
      waitingTuples.add(tuple);
      return;
    }
    super.processTuple(tuple);
  }

  @Override
  protected String getFileName(AbstractBlockReader.ReaderRecord<Slice> tuple)
  {
    MutablePair<Integer,String> blockId = blockInfo.get(tuple.getBlockId());
    return blockId.getRight() + PARTSUFFIX + blockId.getLeft();
  }

  @Override
  public void endWindow()
  {
    processWaitBlocks();
    waitingTuples.clear();
    super.endWindow();
  }

  @Override
  public void finalizeFile(String fileName) throws IOException
  {
    MutablePair<Integer,String> blockId = blockInfo.get(Long.parseLong(fileName));
    super.finalizeFile(blockId.getRight() + PARTSUFFIX + blockId.getLeft());
  }

  @Override
  public void handleIdleTime()
  {
    processWaitBlocks();
  }

  /**
   * Process the blocks which are in wait state.
   */
  protected void processWaitBlocks()
  {
    Iterator<AbstractBlockReader.ReaderRecord<Slice>> waitIterator = waitingTuples.iterator();
    while (waitIterator.hasNext()) {
      AbstractBlockReader.ReaderRecord<Slice> blockData = waitIterator.next();
      if (blockInfo.get(blockData.getBlockId()) != null) {
        super.processTuple(blockData);
        waitIterator.remove();
      }
    }
  }

  /**
   * Return the path of output directory for storing part files
   * @return outputDirectoryPath
   */
  public String getOutputDirectoryPath()
  {
    return outputDirectoryPath;
  }

  /**
   * Specify the path of output directory for storing part files
   * @param outputDirectoryPath given outputDirectoryPath
   */
  public void setOutputDirectoryPath(String outputDirectoryPath)
  {
    this.outputDirectoryPath = outputDirectoryPath;
  }
}
