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

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StatsListener;
import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.validation.constraints.NotNull;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Input operator that reads files from a directory.
 * <p/>
 * Derived class defines how to read entries from the input stream and emit to the port.
 * <p/>
 * The directory scanning logic is pluggable to support custom directory layouts and naming schemes. The default
 * implementation scans a single directory.
 * <p/>
 * Fault tolerant by tracking previously read files and current offset as part of checkpoint state. In case of failure
 * the operator will skip files that were already processed and fast forward to the offset of the current file.
 * <p/>
 * Supports partitioning and dynamic changes to number of partitions through property {@link #partitionCount}. The
 * directory scanner is responsible to only accept the files that belong to a partition.
 * <p/>
 * This class supports retrying of failed files by putting them into failed list, and retrying them after pending
 * files are processed. Retrying is disabled when maxRetryCount is set to zero.
 *
 * @since 1.0.2
 */
public abstract class AbstractFSDirectoryInputOperator<T> implements InputOperator, Partitioner<AbstractFSDirectoryInputOperator<T>>, StatsListener
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractFSDirectoryInputOperator.class);

  @NotNull
  protected String directory;
  @NotNull
  protected DirectoryScanner scanner = new DirectoryScanner();
  protected int scanIntervalMillis = 5000;
  protected int offset;
  protected String currentFile;
  protected Set<String> processedFiles = new HashSet<String>();
  protected int emitBatchSize = 1000;
  protected int currentPartitions = 1 ;
  protected int partitionCount = 1;
  private int retryCount = 0;
  private int maxRetryCount = 5;
  transient protected int skipCount = 0;

  /**
   * Class representing failed file, When read fails on a file in middle, then the file is
   * added to failedList along with last read offset.
   * The files from failedList will be processed after all pendingFiles are processed, but
   * before checking for new files.
   * failed file is retried for maxRetryCount number of times, after that the file is
   * ignored.
   */
  protected static class FailedFile {
    String path;
    int   offset;
    int    retryCount;
    long   lastFailedTime;

    /* For kryo serialization */
    protected FailedFile() {}

    protected FailedFile(String path, int offset) {
      this.path = path;
      this.offset = offset;
      this.retryCount = 0;
    }

    protected FailedFile(String path, int offset, int retryCount) {
      this.path = path;
      this.offset = offset;
      this.retryCount = retryCount;
    }

    @Override
    public String toString()
    {
      return "FailedFile[" +
          "path='" + path + '\'' +
          ", offset=" + offset +
          ", retryCount=" + retryCount +
          ", lastFailedTime=" + lastFailedTime +
          ']';
    }
  }
  
  protected long lastRepartition = 0;
  private transient boolean emit = true;
  protected boolean idempotentEmit = false;
  /* List of unfinished files */
  protected Queue<FailedFile> unfinishedFiles = new LinkedList<FailedFile>();
  /* List of failed file */
  protected Queue<FailedFile> failedFiles = new LinkedList<FailedFile>();

  protected transient FileSystem fs;
  protected transient Configuration configuration;
  protected transient long lastScanMillis;
  protected transient Path filePath;
  protected transient InputStream inputStream;
  protected Set<String> pendingFiles = new LinkedHashSet<String>();

  public String getDirectory()
  {
    return directory;
  }

  public void setDirectory(String directory)
  {
    this.directory = directory;
  }

  public DirectoryScanner getScanner()
  {
    return scanner;
  }

  public void setScanner(DirectoryScanner scanner)
  {
    this.scanner = scanner;
  }

  public int getScanIntervalMillis()
  {
    return scanIntervalMillis;
  }

  public void setScanIntervalMillis(int scanIntervalMillis)
  {
    this.scanIntervalMillis = scanIntervalMillis;
  }

  public int getEmitBatchSize()
  {
    return emitBatchSize;
  }

  public void setEmitBatchSize(int emitBatchSize)
  {
    this.emitBatchSize = emitBatchSize;
  }
  
  public void setIdempotentEmit(boolean idempotentEmit)
  {
    this.idempotentEmit = idempotentEmit;
  }
  
  public boolean isIdempotentEmit()
  {
    return idempotentEmit;
  }
  
  public int getPartitionCount()
  {
    return partitionCount;
  }

  public void setPartitionCount(int requiredPartitions)
  {
    this.partitionCount = requiredPartitions; 
  }

  public int getCurrentPartitions()
  {
    return currentPartitions;
  }

  @Override
  public void setup(OperatorContext context)
  {
    try {
      filePath = new Path(directory);
      configuration = new Configuration();
      fs = FileSystem.newInstance(filePath.toUri(), configuration);
      if(!unfinishedFiles.isEmpty()) {
        retryFailedFile(unfinishedFiles.poll());
        skipCount = 0;
      } else if(!failedFiles.isEmpty()) {
        retryFailedFile(failedFiles.poll());
        skipCount = 0;
      }
      long startTime = System.currentTimeMillis();
      LOG.info("Continue reading {} from index {} time={}", currentFile, offset, startTime);
      // fast forward to previous offset
      if(inputStream != null) {
        for(int index = 0; index < offset; index++) {
          readEntity();
        }
      }
      LOG.info("Read offset={} records in setup time={}", offset, System.currentTimeMillis() - startTime);
    }
    catch (IOException ex) {
      if(maxRetryCount <= 0) {
        throw new RuntimeException(ex);
      }
      LOG.error("FS reader error", ex);
      addToFailedList();
    }
  }

  @Override
  public void teardown()
  {
    IOUtils.closeQuietly(inputStream);
    IOUtils.closeQuietly(fs);
  }

  @Override
  public void beginWindow(long windowId)
  {
    emit = true;
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void emitTuples()
  {
    //emit will be true if the operator is not idempotent. If the operator is 
    //idempotent then emit will be true the first time emitTuples is called
    //within a window and false the other times emit tuples is called within a
    //window
    if(emit)
    {
      if (inputStream == null) {
        try {
          if(!unfinishedFiles.isEmpty()) {
            retryFailedFile(unfinishedFiles.poll());
          }
          else if (!pendingFiles.isEmpty()) {
            String newPathString = pendingFiles.iterator().next();
            pendingFiles.remove(newPathString);
            this.inputStream = openFile(new Path(newPathString));
          }
          else if (!failedFiles.isEmpty()) {
            retryFailedFile(failedFiles.poll());
          }
          else {
            if (System.currentTimeMillis() - scanIntervalMillis >= lastScanMillis) {
              Set<Path> newPaths = scanner.scan(fs, filePath, processedFiles);
      
              for(Path newPath: newPaths) {
                String newPathString = newPath.toString();
                pendingFiles.add(newPathString);
                processedFiles.add(newPathString);
              }
              lastScanMillis = System.currentTimeMillis();
            }
          }
        } catch (IOException ex) {
          if(maxRetryCount <= 0) {
            throw new RuntimeException(ex);
          }
          LOG.error("FS reader error", ex);
          addToFailedList();
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

            // If skipCount is non zero, then failed file recovery is going on, skipCount is
            // used to prevent already emitted records from being emitted again during recovery.
            // When failed file is open, skipCount is set to the last read offset for that file.
            //
            if (skipCount == 0) {
              offset++;
              emit(line);
            }
            else
              skipCount--;
          }
        } catch (IOException e) {
          if(maxRetryCount <= 0) {
            throw new RuntimeException(e);
          }
          LOG.error("FS reader error", e);
          addToFailedList();
        }
      }
      //If the operator is idempotent, do nothing on other calls to emittuples
      //within the same window
      if(idempotentEmit)
      {
        emit = false;
      }
    }
  }

  protected void addToFailedList() {

    FailedFile ff = new FailedFile(currentFile, offset, retryCount);

    try {
      // try to close file
      if (this.inputStream != null)
        this.inputStream.close();
    } catch(IOException e) {
      LOG.error("Could not close input stream on: " + currentFile);
    }

    ff.retryCount ++;
    ff.lastFailedTime = System.currentTimeMillis();
    ff.offset = this.offset;

    // Clear current file state.
    this.currentFile = null;
    this.inputStream = null;
    this.offset = 0;

    if (ff.retryCount > maxRetryCount)
      return;

    LOG.info("adding to failed list path {} offset {} retry {}", ff.path, ff.offset, ff.retryCount);
    failedFiles.add(ff);
  }

  protected InputStream retryFailedFile(FailedFile ff)  throws IOException
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

    if (is != null)
      is.close();

    currentFile = null;
    inputStream = null;
  }

  @Override
  public Collection<Partition<AbstractFSDirectoryInputOperator<T>>> definePartitions(Collection<Partition<AbstractFSDirectoryInputOperator<T>>> partitions, int incrementalCapacity)
  {
    lastRepartition = System.currentTimeMillis();
    
    int totalCount = computedNewPartitionCount(partitions, incrementalCapacity);

    if (totalCount == partitions.size()) {
      return partitions;
    }

    /*
     * Build collective state from all instances of the operator.
     */
    Set<String> totalProcessedFiles = new HashSet<String>();
    Set<FailedFile> currentFiles = new HashSet<FailedFile>();
    List<DirectoryScanner> oldscanners = new LinkedList<DirectoryScanner>();
    List<FailedFile> totalFailedFiles = new LinkedList<FailedFile>();
    List<String> totalPendingFiles = new LinkedList<String>();
    for(Partition<AbstractFSDirectoryInputOperator<T>> partition : partitions) {
      AbstractFSDirectoryInputOperator<T> oper = partition.getPartitionedInstance();
      totalProcessedFiles.addAll(oper.processedFiles);
      totalFailedFiles.addAll(oper.failedFiles);
      totalPendingFiles.addAll(oper.pendingFiles);
      currentFiles.addAll(unfinishedFiles);
      if (oper.currentFile != null)
        currentFiles.add(new FailedFile(oper.currentFile, oper.offset));
      oldscanners.add(oper.getScanner());
    }

    /*
     * Create partitions of scanners, scanner's partition method will do state
     * transfer for DirectoryScanner objects.
     */
    List<DirectoryScanner> scanners = scanner.partition(totalCount, oldscanners);

    Kryo kryo = new Kryo();
    Collection<Partition<AbstractFSDirectoryInputOperator<T>>> newPartitions = Lists.newArrayListWithExpectedSize(totalCount);
    for (int i=0; i<scanners.size(); i++) {
      AbstractFSDirectoryInputOperator<T> oper = kryo.copy(this);
      DirectoryScanner scn = scanners.get(i);
      oper.setScanner(scn);

      // Do state transfer for processed files.
      oper.processedFiles.addAll(totalProcessedFiles);

      /* redistribute unfinished files properly */
      oper.unfinishedFiles.clear();
      oper.currentFile = null;
      oper.offset = 0;
      Iterator<FailedFile> unfinishedIter = currentFiles.iterator();
      while(unfinishedIter.hasNext()) {
        FailedFile unfinishedFile = unfinishedIter.next();
        if (scn.acceptFile(unfinishedFile.path)) {
          oper.unfinishedFiles.add(unfinishedFile);
          unfinishedIter.remove();
        }
      }

      /* transfer failed files */
      oper.failedFiles.clear();
      Iterator<FailedFile> iter = totalFailedFiles.iterator();
      while (iter.hasNext()) {
        FailedFile ff = iter.next();
        if (scn.acceptFile(ff.path)) {
          oper.failedFiles.add(ff);
          iter.remove();
        }
      }

      /* redistribute pending files properly */
      oper.pendingFiles.clear();
      Iterator<String> pendingFilesIterator = totalPendingFiles.iterator();
      while(pendingFilesIterator.hasNext()) {
        String pathString = pendingFilesIterator.next();
        if(scn.acceptFile(pathString)) {
          oper.pendingFiles.add(pathString);
          pendingFilesIterator.remove();
        }
      }
      newPartitions.add(new DefaultPartition<AbstractFSDirectoryInputOperator<T>>(oper));
    }

    LOG.info("definePartitions called returning {} partitions", newPartitions.size());
    return newPartitions;
  }
  
  protected int computedNewPartitionCount(Collection<Partition<AbstractFSDirectoryInputOperator<T>>> partitions, int incrementalCapacity)
  {
    boolean isInitialParitition = partitions.iterator().next().getStats() == null;
    
    if (isInitialParitition && partitionCount == 1) {
      partitionCount = currentPartitions = partitions.size() + incrementalCapacity;
    } else {
      incrementalCapacity = partitionCount - currentPartitions;
    }

    int totalCount = partitions.size() + incrementalCapacity;
    LOG.info("definePartitions trying to create {} partitions, current {}  required {}", totalCount, partitionCount, currentPartitions);
    return totalCount;
  }

  @Override
  public void partitioned(Map<Integer, Partition<AbstractFSDirectoryInputOperator<T>>> partitions)
  {
    currentPartitions = partitions.size();
  }

  /**
   * Read the next item from the stream. Depending on the type of stream, this could be a byte array, line or object.
   * Upon return of null, the stream will be considered fully consumed.
   */
  abstract protected T readEntity() throws IOException;

  /**
   * Emit the tuple on the port
   * @param tuple
   */
  abstract protected void emit(T tuple);


  /**
   * Repartition is required when number of partitions are not equal to required
   * partitions.
   */
  @Override
  public Response processStats(BatchedOperatorStats batchedOperatorStats)
  {
    Response res = new Response();
    res.repartitionRequired = false;
    if (currentPartitions != partitionCount) {
      LOG.info("processStats: trying repartition of input operator current {} required {}", currentPartitions, partitionCount);
      res.repartitionRequired = true;
    }
    return res;
  }

  public int getMaxRetryCount()
  {
    return maxRetryCount;
  }

  public void setMaxRetryCount(int maxRetryCount)
  {
    this.maxRetryCount = maxRetryCount;
  }

  public static class DirectoryScanner implements Serializable
  {
    private static final long serialVersionUID = 4535844463258899929L;
    private String filePatternRegexp;
    private transient Pattern regex = null;
    private int partitionIndex;
    private int partitionCount;
    private final transient HashSet<String> ignoredFiles = new HashSet<String>();

    public String getFilePatternRegexp()
    {
      return filePatternRegexp;
    }

    public void setFilePatternRegexp(String filePatternRegexp)
    {
      this.filePatternRegexp = filePatternRegexp;
      this.regex = null;
    }

    public Pattern getRegex() {
      if (this.regex == null && this.filePatternRegexp != null)
        this.regex = Pattern.compile(this.filePatternRegexp);
      return this.regex;
    }

    public int getPartitionCount() {
      return partitionCount;
    }

    public int getPartitionIndex() {
      return partitionIndex;
    }

    public LinkedHashSet<Path> scan(FileSystem fs, Path filePath, Set<String> consumedFiles)
    {
      if (filePatternRegexp != null && this.regex == null) {
        this.regex = Pattern.compile(this.filePatternRegexp);
      }

      LinkedHashSet<Path> pathSet = Sets.newLinkedHashSet();
      try {
        LOG.debug("Scanning {} with pattern {}", filePath, this.filePatternRegexp);
        FileStatus[] files = fs.listStatus(filePath);
        for (FileStatus status : files)
        {
          Path path = status.getPath();
          String filePathStr = path.toString();

          if (consumedFiles.contains(filePathStr)) {
            continue;
          }

          if (ignoredFiles.contains(filePathStr)) {
            continue;
          }

          if (acceptFile(filePathStr)) {
            LOG.debug("Found {}", filePathStr);
            pathSet.add(path);
          } else {
            // don't look at it again
            ignoredFiles.add(filePathStr);
          }
        }
      } catch (FileNotFoundException e) {
        LOG.warn("Failed to list directory {}", filePath, e);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return pathSet;
    }

    protected boolean acceptFile(String filePathStr)
    {
      if (partitionCount > 1) {
        int i = filePathStr.hashCode();
        int mod = i % partitionCount;
        if (mod < 0) {
          mod += partitionCount;
        }
        LOG.debug("partition {} {} {} {}", partitionIndex, filePathStr, i, mod);

        if (mod != partitionIndex) {
          return false;
        }
      }
      if (filePatternRegexp != null && this.regex == null) {
        regex = Pattern.compile(this.filePatternRegexp);
      }

      if (regex != null)
      {
        Matcher matcher = regex.matcher(filePathStr);
        if (!matcher.matches()) {
          return false;
        }
      }
      return true;
    }

    public List<DirectoryScanner> partition(int count)
    {
      ArrayList<DirectoryScanner> partitions = Lists.newArrayListWithExpectedSize(count);
      for (int i=0; i<count; i++) {
        partitions.add(this.createPartition(i, count));
      }
      return partitions;
    }

    public List<DirectoryScanner>  partition(int count , Collection<DirectoryScanner> scanners) {
      return partition(count);
    }

    protected DirectoryScanner createPartition(int partitionIndex, int partitionCount)
    {
      DirectoryScanner that = new DirectoryScanner();
      that.filePatternRegexp = this.filePatternRegexp;
      that.regex = this.regex;
      that.partitionIndex = partitionIndex;
      that.partitionCount = partitionCount;
      return that;
    }

    @Override
    public String toString()
    {
      return "DirectoryScanner [filePatternRegexp=" + filePatternRegexp + " partitionIndex=" +
          partitionIndex + " partitionCount=" + partitionCount + "]";
    }
  }
}
