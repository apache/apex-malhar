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

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import org.apache.apex.malhar.lib.codec.KryoSerializableStreamCodec;
import org.apache.apex.malhar.lib.io.block.AbstractBlockReader;
import org.apache.apex.malhar.lib.io.block.BlockMetadata;
import org.apache.apex.malhar.lib.io.block.FSSliceReader;
import org.apache.apex.malhar.lib.io.block.ReaderContext;
import org.apache.hadoop.conf.Configuration;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.netlet.util.Slice;

/**
 * FSInputModule is a class used to read files from file systems like HDFS, NFS, S3, etc. <br/>
 * FSInputModule emits FileMetadata, BlockMetadata, BlockBytes. <br/>
 * The module reads data in parallel, following parameters can be configured<br/>
 * 1. files: list of file(s)/directories to read<br/>
 * 2. filePatternRegularExp: Files names matching given regex will be read<br/>
 * 3. scanIntervalMillis: interval between two scans to discover new files in input directory<br/>
 * 4. recursive: if scan recursively input directories<br/>
 * 5. blockSize: block size used to read input blocks of file<br/>
 * 6. sequentialFileRead: If emit file blocks in sequence?<br/>
 * 7. blocksThreshold: number of blocks emitted per window
 * 8. minReaders: Minimum number of block readers for dynamic partitioning
 * 9. maxReaders: Maximum number of block readers for dynamic partitioning
 * 10. repartitionCheckInterval: Interval for re-evaluating dynamic partitioning
 *
 * @since 3.5.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class FSInputModule implements Module
{
  @NotNull
  @Size(min = 1)
  private String files;
  private String filePatternRegularExp;
  @Min(0)
  private long scanIntervalMillis;
  private boolean recursive = true;
  private long blockSize;
  private boolean sequentialFileRead = false;
  @Min(1)
  protected int blocksThreshold;
  protected int minReaders;
  protected int maxReaders;
  protected long repartitionCheckInterval;

  public final transient ProxyOutputPort<AbstractFileSplitter.FileMetadata> filesMetadataOutput = new ProxyOutputPort<>();
  public final transient ProxyOutputPort<BlockMetadata.FileBlockMetadata> blocksMetadataOutput = new ProxyOutputPort<>();
  public final transient ProxyOutputPort<AbstractBlockReader.ReaderRecord<Slice>> messages = new ProxyOutputPort<>();

  public FileSplitterInput createFileSplitter()
  {
    return new FileSplitterInput();
  }

  public FSSliceReader createBlockReader()
  {
    return new FSSliceReader();
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FileSplitterInput fileSplitter = dag.addOperator("FileSplitter", createFileSplitter());
    FSSliceReader blockReader = dag.addOperator("BlockReader", createBlockReader());

    dag.addStream("BlockMetadata", fileSplitter.blocksMetadataOutput, blockReader.blocksMetadataInput);

    filesMetadataOutput.set(fileSplitter.filesMetadataOutput);
    blocksMetadataOutput.set(blockReader.blocksMetadataOutput);
    messages.set(blockReader.messages);

    if (sequentialFileRead) {
      dag.setInputPortAttribute(blockReader.blocksMetadataInput, Context.PortContext.STREAM_CODEC,
          new SequentialFileBlockMetadataCodec());
    }
    if (blockSize != 0) {
      fileSplitter.setBlockSize(blockSize);
      if (blockReader.getReaderContext() instanceof ReaderContext.FixedBytesReaderContext) {
        ((ReaderContext.FixedBytesReaderContext)blockReader.getReaderContext()).setLength((int)blockSize);
      }
    }

    FileSplitterInput.TimeBasedDirectoryScanner fileScanner = fileSplitter.getScanner();
    fileScanner.setFiles(files);
    if (scanIntervalMillis != 0) {
      fileScanner.setScanIntervalMillis(scanIntervalMillis);
    }
    fileScanner.setRecursive(recursive);
    if (filePatternRegularExp != null) {
      fileSplitter.getScanner().setFilePatternRegularExp(filePatternRegularExp);
    }

    blockReader.setBasePath(files);

    if (minReaders != 0) {
      blockReader.setMinReaders(minReaders);
    }

    if (maxReaders != 0) {
      blockReader.setMaxReaders(maxReaders);
    }

    if (repartitionCheckInterval != 0) {
      blockReader.setIntervalMillis(repartitionCheckInterval);
    }
    fileSplitter.setBlocksThreshold(blocksThreshold);
  }

  /**
   * A comma separated list of directories to scan. If the path is not fully qualified the default file system is used.
   * A fully qualified path can be provided to scan directories in other filesystems.
   *
   * @param files
   *          files
   */
  public void setFiles(String files)
  {
    this.files = files;
  }

  /**
   * Gets the files to be scanned.
   *
   * @return files to be scanned.
   */
  public String getFiles()
  {
    return files;
  }

  /**
   * Gets the regular expression for file names to split
   *
   * @return regular expression
   */
  public String getFilePatternRegularExp()
  {
    return filePatternRegularExp;
  }

  /**
   * Only files with names matching the given java regular expression are split
   *
   * @param filePatternRegexp
   *          regular expression
   */
  public void setFilePatternRegularExp(String filePatternRegexp)
  {
    this.filePatternRegularExp = filePatternRegexp;
  }

  /**
   * Gets scan interval in milliseconds, interval between two scans to discover new files in input directory
   *
   * @return scanInterval milliseconds
   */
  public long getScanIntervalMillis()
  {
    return scanIntervalMillis;
  }

  /**
   * Sets scan interval in milliseconds, interval between two scans to discover new files in input directory
   *
   * @param scanIntervalMillis
   */
  public void setScanIntervalMillis(long scanIntervalMillis)
  {
    this.scanIntervalMillis = scanIntervalMillis;
  }

  /**
   * Get is scan recursive
   *
   * @return isRecursive
   */
  public boolean isRecursive()
  {
    return recursive;
  }

  /**
   * set is scan recursive
   *
   * @param recursive
   */
  public void setRecursive(boolean recursive)
  {
    this.recursive = recursive;
  }

  /**
   * Get block size used to read input blocks of file
   *
   * @return blockSize
   */
  public long getBlockSize()
  {
    return blockSize;
  }

  /**
   * Sets block size used to read input blocks of file
   *
   * @param blockSize
   */
  public void setBlockSize(long blockSize)
  {
    this.blockSize = blockSize;
  }

  /**
   * Gets is sequential file read
   *
   * @return sequentialFileRead
   */
  public boolean isSequentialFileRead()
  {
    return sequentialFileRead;
  }

  /**
   * Sets is sequential file read
   *
   * @param sequentialFileRead
   */
  public void setSequentialFileRead(boolean sequentialFileRead)
  {
    this.sequentialFileRead = sequentialFileRead;
  }

  /**
   * Sets number of blocks to be emitted per window.<br/>
   * A lot of blocks emitted per window can overwhelm the downstream operators. Set this value considering blockSize and
   * readersCount.
   * @param threshold
   */
  public void setBlocksThreshold(int threshold)
  {
    this.blocksThreshold = threshold;
  }

  /**
   * Gets number of blocks to be emitted per window.<br/>
   * A lot of blocks emitted per window can overwhelm the downstream operators. Set this value considering blockSize and
   * readersCount.
   * @return
   */
  public int getBlocksThreshold()
  {
    return blocksThreshold;
  }

  /**
   * Gets minimum number of block readers for dynamic partitioning.
   * @return minimum instances of block reader.
   */
  public int getMinReaders()
  {
    return minReaders;
  }

  /**
   * Sets minimum number of block readers for dynamic partitioning.
   * @param minReaders minimum number of readers.
   */
  public void setMinReaders(int minReaders)
  {
    this.minReaders = minReaders;
  }

  /**
   * Gets maximum number of block readers for dynamic partitioning.
   * @return maximum instances of block reader.
   */
  public int getMaxReaders()
  {
    return maxReaders;
  }

  /**
   * Sets maximum number of block readers for dynamic partitioning.
   * @param maxReaders maximum number of readers.
   */
  public void setMaxReaders(int maxReaders)
  {
    this.maxReaders = maxReaders;
  }

  /**
   * Gets Interval for re-evaluating dynamic partitioning
   * @return interval for re-evaluating dynamic partitioning
   */
  public long getRepartitionCheckInterval()
  {
    return repartitionCheckInterval;
  }

  /**
   * Sets Interval for re-evaluating dynamic partitioning
   * @param repartitionCheckInterval interval for re-evaluating dynamic partitioning
   */
  public void setRepartitionCheckInterval(long repartitionCheckInterval)
  {
    this.repartitionCheckInterval = repartitionCheckInterval;
  }

  public static class SequentialFileBlockMetadataCodec
      extends KryoSerializableStreamCodec<BlockMetadata.FileBlockMetadata>
  {
    @Override
    public int getPartition(BlockMetadata.FileBlockMetadata fileBlockMetadata)
    {
      return fileBlockMetadata.getFilePath().hashCode();
    }
  }
}
