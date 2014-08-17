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
package com.datatorrent.lib.io.fs;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StatsListener;
import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.validation.constraints.NotNull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractThroughputFSDirectoryInputOperator<T> extends DirectoryReader implements InputOperator, StatsListener, Partitioner<AbstractThroughputFSDirectoryInputOperator<T>>
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractFSDirectoryInputOperator.class);

  ////Created State
  
  private Set<String> processedFiles = null;
  private final Queue<String> pendingFiles = new LinkedList<String>();
  private int offset;
  private String currentFile;
  private int retryCount = 0;
  transient protected int skipCount = 0;
  private transient long lastTime = 0;
  private transient boolean firstStat = true;
  private final Queue<FailedFile> failedFiles = new LinkedList<FailedFile>();
  private transient InputStream inputStream;
  protected transient Pattern regex = null;
  
  //// User defined parameters
  
  private int emitBatchSize = 1000;
  private int maxRetryCount = 5;
  private long scanIntervalMillis = 10L * 1000L;
  private int maxNumberOfOperators = 100;
  private int preferredMaxPendingFilesPerOperator = 10;
  protected String filePatternRegexp;
  
  //// Abstract methods
  
  /**
   * Read the next item from the stream. Depending on the type of stream, this
   * could be a byte array, line or object. Upon return of null, the stream will
   * be considered fully consumed.
   */
  abstract protected T readEntity() throws IOException;

  /**
   * Emit the tuple on the port
   *
   * @param tuple
   */
  abstract protected void emit(T tuple);
  
  //// Setters and getters for user defined parameters
  
  public int getEmitBatchSize()
  {
    return emitBatchSize;
  }

  public void setEmitBatchSize(int emitBatchSize)
  {
    this.emitBatchSize = emitBatchSize;
  }
  
  public int getMaxRetryCount()
  {
    return maxRetryCount;
  }

  public void setMaxRetryCount(int maxRetryCount)
  {
    this.maxRetryCount = maxRetryCount;
  }

  public void setScanIntervalMillis(long scanIntervalMillis)
  {
    this.scanIntervalMillis = scanIntervalMillis;
  }

  public long getScanIntervalMillis()
  {
    return scanIntervalMillis;
  }

  public void setMaxNumberOfOperators(int maxNumberOfOperators)
  {
    this.maxNumberOfOperators = maxNumberOfOperators;
  }

  public int getMaxNumberOfOperators()
  {
    return maxNumberOfOperators;
  }

  public void setPreferredMaxPendingFilesPerOperator(int pendingFilesPerOperator)
  {
    this.preferredMaxPendingFilesPerOperator = pendingFilesPerOperator;
  }

  public int getPreferredMaxPendingFilesPerOperator()
  {
    return preferredMaxPendingFilesPerOperator;
  }
  
  public void setFilePatternRegexp(String filePatternRegexp)
  {
    this.filePatternRegexp = filePatternRegexp;
    this.regex = null;
  }
  
  //// Setters and getters for created state
  
  public final Set<String> getProcessedFiles()
  {
      return processedFiles;
  }
  
  public final Queue<String> getPendingFiles()
  {
      return pendingFiles;
  }
  
  public final Pattern getRegex()
  {
    if(this.regex == null && this.filePatternRegexp != null)
    {
      this.regex = Pattern.compile(this.filePatternRegexp);
    }
    return this.regex;
  }
  
  //// Helper Methods
  
  public Set<String> scanDirectories(FileSystem fs, Path filePath) throws IOException
  {
    LOG.info("Scanning");
    Set<String> scannedFiles = new HashSet<String>();

    FileStatus[] files = fs.listStatus(filePath);

    if(this.getRegex() != null)
    {
      for(FileStatus status : files)
      {
        Path path = status.getPath();
        String filePathStr = path.toString();

        Matcher matcher = regex.matcher(filePathStr);

        if(!matcher.matches())
        {
          continue;
        }

        scannedFiles.add(filePathStr);
      }
    }
    else
    {
      for(FileStatus status : files)
      {
        Path path = status.getPath();
        String filePathStr = path.toString();
        
        LOG.info(filePathStr);
        scannedFiles.add(filePathStr);
      }
    }
    
    fs.close();
    
    return scannedFiles;
  }
  
  protected void addToFailedList() throws IOException
  {

    FailedFile ff = new FailedFile(currentFile, offset, retryCount);

    // try to close file
    if(this.inputStream != null)
    {
      this.inputStream.close();
    }

    ff.retryCount++;
    ff.lastFailedTime = System.currentTimeMillis();
    ff.offset = this.offset;

    // Clear current file state.
    this.currentFile = null;
    this.inputStream = null;
    this.offset = 0;

    if(ff.retryCount > maxRetryCount)
    {
      return;
    }

    LOG.info("adding to failed list path {} offset {} retry {}", ff.path, ff.offset, ff.retryCount);
    failedFiles.add(ff);
  }

  protected InputStream retryFailedFile(FailedFile ff) throws IOException
  {
    LOG.info("retrying failed file {} offset {} retry {}", ff.path, ff.offset, ff.retryCount);
    String path = ff.path;
    this.inputStream = openFile(new Path(path));
    this.offset = ff.offset;
    this.retryCount = ff.retryCount;
    this.skipCount = ff.offset;
    return this.inputStream;
  }

  protected InputStream openFile(Path path) throws IOException
  {
    LOG.info("opening file {}", path);
    InputStream input = fs.open(path);
    currentFile = path.toString();
    offset = 0;
    retryCount = 0;
    skipCount = 0;
    return input;
  }

  protected void closeFile(InputStream is) throws IOException
  {
    LOG.info("closing file {} offset {}", currentFile, offset);

    if(is != null)
    {
      is.close();
    }

    currentFile = null;
    inputStream = null;
  }
  
  //// Overridden methods

  @Override
  public void setup(Context.OperatorContext context)
  {
    LOG.info("Setting up operator");
    
    try
    {
      this.directorySetup();

      if(currentFile != null && offset > 0)
      {
        long startTime = System.currentTimeMillis();
        LOG.info("Continue reading {} from index {} time={}", currentFile, offset, startTime);
        int index = offset;
        this.inputStream = openFile(new Path(currentFile));
        // fast forward to previous offset
        while(offset < index)
        {
          readEntity();
          offset++;
        }
        LOG.info("Read offset={} records in setup time={}", offset, System.currentTimeMillis() - startTime);
      }
    }
    catch(IOException ex)
    {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void teardown()
  {
    try {
      if (inputStream != null) {
        inputStream.close();
      }
      this.directoryTearDown();
    } catch (Exception e) {
      // ignore
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    //Do nothing
  }

  @Override
  public void endWindow()
  {
    //Do nothing
  }
  
  @Override
  final public void emitTuples()
  {
    if (inputStream == null) {

      try {
        if (!pendingFiles.isEmpty()) {
          String path = pendingFiles.poll();
          this.inputStream = openFile(new Path(path));
        }
        else if (!failedFiles.isEmpty()) {
          FailedFile ff = failedFiles.poll();
          this.inputStream = retryFailedFile(ff);
        }
      }catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    if (inputStream != null) {
      try {
        int counterForTuple = 0;
        while (counterForTuple++ < emitBatchSize) {
          T line = readEntity();
          if (line == null) {
            LOG.info("done reading file ({} entries).", offset);
            closeFile(inputStream);
            break;
          }

          /* If skipCount is non zero, then failed file recovery is going on, skipCount is
           * used to prevent already emitted records from being emitted again during recovery.
           * When failed file is open, skipCount is set to the last read offset for that file.
           */
          if (skipCount == 0) {
            offset++;
            emit(line);
          }
          else
            skipCount--;
        }
      } catch (IOException e) {
        LOG.error("FS reader error", e);
        try {
            addToFailedList();
        } catch(IOException e1) {
            throw new RuntimeException("FS reader error", e1);
        }
      }
    }
  }

  @Override
  public StatsListener.Response processStats(StatsListener.BatchedOperatorStats bos)
  {
    LOG.info("Stats Listener");
    
    if(System.currentTimeMillis() - scanIntervalMillis > lastTime)
    {
      StatsListener.Response response = new StatsListener.Response();
      response.repartitionRequired = true;

      return response;
    }

    return new StatsListener.Response();
  }

  @Override
  public Collection<Partition<AbstractThroughputFSDirectoryInputOperator<T>>> definePartitions(Collection<Partition<AbstractThroughputFSDirectoryInputOperator<T>>> partitions, int i)
  {
    lastTime = System.currentTimeMillis();
    firstStat = false;

    //If there is one, get the main operator that saves our processed files
    AbstractThroughputFSDirectoryInputOperator<T> mainOperator = null;

    for(Partition<AbstractThroughputFSDirectoryInputOperator<T>> partition : partitions)
    {
      AbstractThroughputFSDirectoryInputOperator<T> operator = partition.getPartitionedInstance();

      if(operator.getProcessedFiles() != null)
      {
        mainOperator = operator;
        break;
      }
    }

    //Get the processedFiles up to this point, and if there was no
    //No main operator before pick one to be main
    Set<String> tempProcessedFiles;

    if(mainOperator == null)
    {
      tempProcessedFiles = new HashSet<String>();
      mainOperator = partitions.iterator().next().getPartitionedInstance();
    }
    else
    {
      tempProcessedFiles = mainOperator.getProcessedFiles();
    }

    String directory = mainOperator.getDirectory();
    List<String> tempPendingFiles = new ArrayList<String>();
    Set<String> scannedFiles = null;
    
    try
    {
      Path filePath = new Path(directory);
      Configuration configuration = new Configuration();
      FileSystem fs = FileSystem.newInstance(filePath.toUri(), configuration);

      scannedFiles = scanDirectories(fs, filePath);

      fs.close();
    }
    catch(IOException ex)
    {
      throw new RuntimeException(ex);
    }
    
    
    scannedFiles.removeAll(tempProcessedFiles);
    tempPendingFiles.addAll(scannedFiles);
    tempProcessedFiles.addAll(scannedFiles);

        ////
    List<FailedFile> tempFailedFiles = new ArrayList<FailedFile>();

    for(Partition<AbstractThroughputFSDirectoryInputOperator<T>> partition : partitions)
    {
      AbstractThroughputFSDirectoryInputOperator<T> operator = partition.getPartitionedInstance();

      tempPendingFiles.addAll(operator.getPendingFiles());
      tempFailedFiles.addAll(operator.failedFiles);

      if(operator.currentFile != null)
      {
        tempFailedFiles.add(new FailedFile(operator.currentFile,
          operator.offset,
          0));
      }
    }

        ////
    int totalUnfinishedFiles = tempPendingFiles.size()
      + tempFailedFiles.size();

    int newOperatorCount = totalUnfinishedFiles / preferredMaxPendingFilesPerOperator;

    if(totalUnfinishedFiles % preferredMaxPendingFilesPerOperator > 0)
    {
      newOperatorCount++;
    }
    
    Kryo kryo = new Kryo();
    
    if(newOperatorCount == 0)
    {
      Collection<Partition<AbstractThroughputFSDirectoryInputOperator<T>>> newPartitions = Lists.newArrayListWithExpectedSize(1);

      AbstractThroughputFSDirectoryInputOperator<T> operator = kryo.copy(this);
      newPartitions.add(new DefaultPartition<AbstractThroughputFSDirectoryInputOperator<T>>(operator));
      
      operator.processedFiles = tempProcessedFiles;
      operator.currentFile = null;
      operator.offset = 0;
      operator.pendingFiles.clear();
      operator.failedFiles.clear();
      
      return newPartitions;
    }

    if(newOperatorCount > maxNumberOfOperators)
    {
      newOperatorCount = maxNumberOfOperators;
    }

    int pendingFilePerOperator = tempPendingFiles.size() / newOperatorCount;
    int sparePendingFiles = tempPendingFiles.size() % newOperatorCount;

    int failedFilePerOperator = tempFailedFiles.size() / newOperatorCount;
    int spareFailedFiles = tempFailedFiles.size() % newOperatorCount;

    //Allocate new Operators
    
    Collection<Partition<AbstractThroughputFSDirectoryInputOperator<T>>> newPartitions = Lists.newArrayListWithExpectedSize(newOperatorCount);

    for(int operatorCounter = 0,
      pendingFilesCounter = 0,
      failedFilesCounter = 0,
      sparePendingFilesCounter = 0,
      spareFailedFilesCounter = 0;
        operatorCounter < newOperatorCount;
        operatorCounter++)
    {
      AbstractThroughputFSDirectoryInputOperator<T> operator = kryo.copy(this);
      newPartitions.add(new DefaultPartition<AbstractThroughputFSDirectoryInputOperator<T>>(operator));

      if(operatorCounter == 0)
      {
        operator.processedFiles = tempProcessedFiles;
      }
      else
      {
        operator.processedFiles = null;
      }

      operator.currentFile = null;
      operator.offset = 0;

      operator.pendingFiles.clear();

      for(int pendingCounter = 0;
          pendingCounter < pendingFilePerOperator;
          pendingCounter++, pendingFilesCounter++)
      {
        operator.pendingFiles.add(tempPendingFiles.get(pendingFilesCounter));
      }

      if(sparePendingFilesCounter < sparePendingFiles)
      {
        operator.pendingFiles.add(tempPendingFiles.get(pendingFilesCounter));

        pendingFilesCounter++;
        sparePendingFilesCounter++;
      }

      operator.failedFiles.clear();

      for(int failedCounter = 0;
          failedCounter < failedFilePerOperator;
          failedCounter++, failedFilesCounter++)
      {
        operator.failedFiles.add(tempFailedFiles.get(failedCounter));

        failedFilesCounter++;
        spareFailedFilesCounter++;
      }
    }

    return newPartitions;
  }

  @Override
  public void partitioned(Map<Integer, Partition<AbstractThroughputFSDirectoryInputOperator<T>>> map)
  {
  }
}
