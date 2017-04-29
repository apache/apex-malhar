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

package org.apache.apex.malhar.lib.fs;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;

import org.apache.apex.malhar.lib.fs.FSRecordReader.RECORD_READER_MODE;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.lib.io.fs.FSInputModule.SequentialFileBlockMetadataCodec;
import com.datatorrent.lib.io.fs.FileSplitterInput;

/**
 * This module is used for reading records/tuples from FileSystem. Records can
 * be read in parallel using multiple partitions of record reader operator.
 * (Ordering is not guaranteed when records are read in parallel)
 *
 * Input directory is scanned at specified interval to poll for new data.
 *
 * The module reads data in parallel, following parameters can be configured
 * <br/>
 * 1. files: list of file(s)/directories to read<br/>
 * 2. filePatternRegularExp: Files with names matching given regex will be read
 * <br/>
 * 3. scanIntervalMillis: interval between two scans to discover new files in
 * input directory<br/>
 * 4. recursive: if true, scan input directories recursively<br/>
 * 5. blockSize: block size used to read input blocks of file<br/>
 * 6. sequentialFileRead: if true, then each reader partition will read
 * different file. <br/>
 * instead of reading different offsets of the same file. <br/>
 * (File level parallelism instead of block level parallelism)<br/>
 * 7. blocksThreshold: number of blocks emitted per window<br/>
 * 8. minReaders: Minimum number of block readers for dynamic partitioning<br/>
 * 9. maxReaders: Maximum number of block readers for dynamic partitioning<br/>
 * 10. repartitionCheckInterval: Interval for re-evaluating dynamic partitioning<br/>
 * @since 3.5.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class FSRecordReaderModule implements Module
{
  @NotNull
  @Size(min = 1)
  private String files;
  private String filePatternRegularExp;
  @Min(1)
  private long scanIntervalMillis = 5000;
  private boolean recursive = true;
  private boolean sequentialFileRead = false;
  @Min(0)
  private long blockSize;
  @Min(1)
  protected int blocksThreshold = 1;
  protected int minReaders;
  protected int maxReaders;
  protected long repartitionCheckInterval;

  public final transient ProxyOutputPort<byte[]> records = new ProxyOutputPort<byte[]>();

  /**
   * Criteria for record split
   */
  private String mode = RECORD_READER_MODE.DELIMITED_RECORD.toString();

  /**
   * Length for fixed width record
   */
  private int recordLength;

  /**
   * Creates an instance of FileSplitter
   *
   * @return
   */
  public FileSplitterInput createFileSplitter()
  {
    return new FileSplitterInput();
  }

  /**
   * Creates an instance of Record Reader
   *
   * @return FSRecordReader instance
   */
  public FSRecordReader createRecordReader()
  {
    FSRecordReader recordReader = new FSRecordReader();
    recordReader.setMode(mode);
    recordReader.setRecordLength(recordLength);

    return recordReader;
  }

  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    FileSplitterInput fileSplitter = dag.addOperator("FileSplitter", createFileSplitter());
    FSRecordReader recordReader = dag.addOperator("BlockReader", createRecordReader());

    dag.addStream("BlockMetadata", fileSplitter.blocksMetadataOutput, recordReader.blocksMetadataInput);

    if (sequentialFileRead) {
      dag.setInputPortAttribute(recordReader.blocksMetadataInput, Context.PortContext.STREAM_CODEC,
          new SequentialFileBlockMetadataCodec());
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
    recordReader.setBasePath(files);
    fileSplitter.setBlocksThreshold(blocksThreshold);

    if (minReaders != 0) {
      recordReader.setMinReaders(minReaders);
    }
    if (maxReaders != 0) {
      recordReader.setMaxReaders(maxReaders);
    }
    if (repartitionCheckInterval != 0) {
      recordReader.setIntervalMillis(repartitionCheckInterval);
    }

    /**
     * Override the split size or input blocks of a file. If not specified,
     * it would use default blockSize of the filesystem.
     */
    if (blockSize != 0) {
      fileSplitter.setBlockSize(blockSize);
    }
    records.set(recordReader.records);
  }

  /**
   * A comma separated list of directories to scan. If the path is not fully
   * qualified the default file system is used. A fully qualified path can be
   * provided to scan directories in other filesystems.
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
   * Only files width names matching the given java regular expression are split
   *
   * @param filePatternRegexp
   *          regular expression
   */
  public void setFilePatternRegularExp(String filePatternRegexp)
  {
    this.filePatternRegularExp = filePatternRegexp;
  }

  /**
   * Gets scan interval in milliseconds, interval between two scans to discover
   * new files in input directory
   *
   * @return scanInterval milliseconds
   */
  public long getScanIntervalMillis()
  {
    return scanIntervalMillis;
  }

  /**
   * Sets scan interval in milliseconds, interval between two scans to discover
   * new files in input directory
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
   * A lot of blocks emitted per window can overwhelm the downstream operators.
   * Set this value considering blockSize, minReaders and maxReaders.
   *
   * @param threshold
   */
  public void setBlocksThreshold(int threshold)
  {
    this.blocksThreshold = threshold;
  }

  /**
   * Gets number of blocks to be emitted per window.<br/>
   * A lot of blocks emitted per window can overwhelm the downstream operators.
   * Set this value considering blockSize, minReaders and maxReaders.
   *
   * @return blocksThreshold
   */
  public int getBlocksThreshold()
  {
    return blocksThreshold;
  }

  /**
   * Gets block size used to read input blocks of file
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
   * Gets minimum number of block readers for dynamic partitioning.
   *
   * @return minimum instances of block reader.
   */
  public int getMinReaders()
  {
    return minReaders;
  }

  /**
   * Sets minimum number of block readers for dynamic partitioning.
   *
   * @param minReaders minimum number of readers.
   */
  public void setMinReaders(int minReaders)
  {
    this.minReaders = minReaders;
  }

  /**
   * Gets maximum number of block readers for dynamic partitioning.
   *
   * @return maximum instances of block reader.
   */
  public int getMaxReaders()
  {
    return maxReaders;
  }

  /**
   * Sets maximum number of block readers for dynamic partitioning.
   *
   * @param maxReaders maximum number of readers.
   */
  public void setMaxReaders(int maxReaders)
  {
    this.maxReaders = maxReaders;
  }

  /**
   * Gets Interval for re-evaluating dynamic partitioning.
   *
   * @return interval for re-evaluating dynamic partitioning
   */
  public long getRepartitionCheckInterval()
  {
    return repartitionCheckInterval;
  }

  /**
   * Sets Interval for re-evaluating dynamic partitioning.
   *
   * @param repartitionCheckInterval interval for re-evaluating dynamic partitioning
   */
  public void setRepartitionCheckInterval(long repartitionCheckInterval)
  {
    this.repartitionCheckInterval = repartitionCheckInterval;
  }

  /**
   * Criteria for record split : FIXED_WIDTH_RECORD or DELIMITED_RECORD
   *
   * @param mode
   *          Mode
   */
  public void setMode(
      @Pattern(regexp = "FIXED_WIDTH_RECORD|DELIMITED_RECORD", flags = Pattern.Flag.CASE_INSENSITIVE) String mode)
  {
    this.mode = mode;
  }

  /**
   * Criteria for record split
   *
   * @return mode
   */
  public String getMode()
  {
    return mode;
  }

  /**
   * Length for fixed width record
   *
   * @return record length
   */
  public int getRecordLength()
  {
    return recordLength;
  }

  /**
   * Length for fixed width record
   *
   * @param recordLength
   */
  public void setRecordLength(int recordLength)
  {
    this.recordLength = recordLength;
  }
}
