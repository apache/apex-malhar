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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.counters.BasicCounters;
import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;
import org.apache.apex.malhar.lib.util.KryoCloneUtils;
import org.apache.apex.malhar.lib.wal.WindowDataManager;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context.CountersAggregator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StatsListener;

/**
 * This is the base implementation of a directory input operator, which scans a directory for files.&nbsp;
 * Files are then read and split into tuples, which are emitted.&nbsp;
 * Subclasses should implement the methods required to read and emit tuples from files.
 * <p>
 * Derived class defines how to read entries from the input stream and emit to the port.
 * </p>
 * <p>
 * The directory scanning logic is pluggable to support custom directory layouts and naming schemes. The default
 * implementation scans a single directory.
 * </p>
 * <p>
 * Fault tolerant by tracking previously read files and current offset as part of checkpoint state. In case of failure
 * the operator will skip files that were already processed and fast forward to the offset of the current file.
 * </p>
 * <p>
 * Supports partitioning and dynamic changes to number of partitions through property {@link #partitionCount}. The
 * directory scanner is responsible to only accept the files that belong to a partition.
 * </p>
 * <p>
 * This class supports retrying of failed files by putting them into failed list, and retrying them after pending
 * files are processed. Retrying is disabled when maxRetryCount is set to zero.
 * </p>
 * @displayName FS Directory Scan Input
 * @category Input
 * @tags fs, file, input operator
 *
 * @param <T> The type of the object that this input operator reads.
 * @since 1.0.2
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class AbstractFileInputOperator<T> implements InputOperator, Partitioner<AbstractFileInputOperator<T>>, StatsListener,
    Operator.CheckpointListener, Operator.CheckpointNotificationListener
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractFileInputOperator.class);

  @NotNull
  protected String directory;
  @NotNull
  protected DirectoryScanner scanner = new DirectoryScanner();
  @Min(0)
  protected int scanIntervalMillis = 5000;
  protected long offset;
  protected String currentFile;
  protected Set<String> processedFiles = new HashSet<String>();
  @Min(1)
  protected int emitBatchSize = 1000;
  protected int currentPartitions = 1;
  protected int partitionCount = 1;
  private int retryCount = 0;
  @Min(0)
  private int maxRetryCount = 5;
  protected transient long skipCount = 0;
  private transient OperatorContext context;

  private final BasicCounters<MutableLong> fileCounters = new BasicCounters<MutableLong>(MutableLong.class);
  protected MutableLong globalNumberOfFailures = new MutableLong();
  protected MutableLong localNumberOfFailures = new MutableLong();
  protected MutableLong globalNumberOfRetries = new MutableLong();
  protected MutableLong localNumberOfRetries = new MutableLong();
  protected transient MutableLong globalProcessedFileCount = new MutableLong();
  protected transient MutableLong localProcessedFileCount = new MutableLong();
  protected transient MutableLong pendingFileCount = new MutableLong();

  @NotNull
  private WindowDataManager windowDataManager = new WindowDataManager.NoopWindowDataManager();
  protected transient long currentWindowId;
  protected final transient LinkedList<RecoveryEntry> currentWindowRecoveryState = Lists.newLinkedList();
  protected int operatorId; //needed in partitioning

  /**
   * Class representing failed file, When read fails on a file in middle, then the file is
   * added to failedList along with last read offset.
   * The files from failedList will be processed after all pendingFiles are processed, but
   * before checking for new files.
   * failed file is retried for maxRetryCount number of times, after that the file is
   * ignored.
   */
  protected static class FailedFile
  {
    String path;
    long   offset;
    int    retryCount;
    long   lastFailedTime;

    /* For kryo serialization */
    @SuppressWarnings("unused")
    protected FailedFile() {}

    protected FailedFile(String path, long offset)
    {
      this.path = path;
      this.offset = offset;
      this.retryCount = 0;
    }

    protected FailedFile(String path, long offset, int retryCount)
    {
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

  /**
   * Enums for aggregated counters about file processing.
   * <p/>
   * Contains the enums representing number of files processed, number of
   * pending files, number of file errors, and number of retries.
   * <p/>
   * @since 1.0.4
   */
  public static enum AggregatedFileCounters
  {
    /**
     * The number of files processed by the logical operator up until this.
     * point in time
     */
    PROCESSED_FILES,
    /**
     * The number of files waiting to be processed by the logical operator.
     */
    PENDING_FILES,
    /**
     * The number of IO errors encountered by the logical operator.
     */
    NUMBER_OF_ERRORS,
    /**
     * The number of times the logical operator tried to resume reading a file
     * on which it encountered an error.
     */
    NUMBER_OF_RETRIES
  }

  /**
   * The enums used to track statistics about the
   * AbstractFSDirectoryInputOperator.
   */
  protected static enum FileCounters
  {
    /**
     * The number of files that were in the processed list up to the last
     * repartition of the operator.
     */
    GLOBAL_PROCESSED_FILES,
    /**
     * The number of files added to the processed list by the physical operator
     * since the last repartition.
     */
    LOCAL_PROCESSED_FILES,
    /**
     * The number of io errors encountered up to the last repartition of the
     * operator.
     */
    GLOBAL_NUMBER_OF_FAILURES,
    /**
     * The number of failures encountered by the physical operator since the
     * last repartition.
     */
    LOCAL_NUMBER_OF_FAILURES,
    /**
     * The number of retries encountered by the physical operator up to the last
     * repartition.
     */
    GLOBAL_NUMBER_OF_RETRIES,
    /**
     * The number of retries encountered by the physical operator since the last
     * repartition.
     */
    LOCAL_NUMBER_OF_RETRIES,
    /**
     * The number of files pending on the physical operator.
     */
    PENDING_FILES
  }

  /**
   * A counter aggregator for AbstractFSDirectoryInputOperator.
   * <p/>
   * In order for this CountersAggregator to be used on your operator, you must
   * set it within your application like this.
   * <p/>
   * <code>
   * dag.getOperatorMeta("fsinputoperator").getAttributes().put(OperatorContext.COUNTERS_AGGREGATOR,
   *                                                            new AbstractFSDirectoryInputOperator.FileCountersAggregator());
   * </code>
   * <p/>
   * The value of the aggregated counter can be retrieved by issuing a get
   * request to the host running your gateway like this.
   * <p/>
   * <code>
   * http://&lt;your host&gt;:9090/ws/v2/applications/&lt;your app id&gt;/logicalPlan/operators/&lt;operatorname&gt;/aggregation
   * </code>
   * <p/>
   * @since 1.0.4
   */
  public static final class FileCountersAggregator implements CountersAggregator, Serializable
  {
    private static final long serialVersionUID = 201409041428L;
    MutableLong totalLocalProcessedFiles = new MutableLong();
    MutableLong pendingFiles = new MutableLong();
    MutableLong totalLocalNumberOfFailures = new MutableLong();
    MutableLong totalLocalNumberOfRetries = new MutableLong();

    @Override
    @SuppressWarnings("unchecked")
    public Object aggregate(Collection<?> countersList)
    {
      if (countersList.isEmpty()) {
        return null;
      }

      BasicCounters<MutableLong> tempFileCounters = (BasicCounters<MutableLong>)countersList.iterator().next();
      MutableLong globalProcessedFiles = tempFileCounters.getCounter(FileCounters.GLOBAL_PROCESSED_FILES);
      MutableLong globalNumberOfFailures = tempFileCounters.getCounter(FileCounters.GLOBAL_NUMBER_OF_FAILURES);
      MutableLong globalNumberOfRetries = tempFileCounters.getCounter(FileCounters.GLOBAL_NUMBER_OF_RETRIES);
      totalLocalProcessedFiles.setValue(0);
      pendingFiles.setValue(0);
      totalLocalNumberOfFailures.setValue(0);
      totalLocalNumberOfRetries.setValue(0);

      for (Object fileCounters : countersList) {
        BasicCounters<MutableLong> basicFileCounters = (BasicCounters<MutableLong>)fileCounters;
        totalLocalProcessedFiles.add(basicFileCounters.getCounter(FileCounters.LOCAL_PROCESSED_FILES));
        pendingFiles.add(basicFileCounters.getCounter(FileCounters.PENDING_FILES));
        totalLocalNumberOfFailures.add(basicFileCounters.getCounter(FileCounters.LOCAL_NUMBER_OF_FAILURES));
        totalLocalNumberOfRetries.add(basicFileCounters.getCounter(FileCounters.LOCAL_NUMBER_OF_RETRIES));
      }

      globalProcessedFiles.add(totalLocalProcessedFiles);
      globalProcessedFiles.subtract(pendingFiles);
      globalNumberOfFailures.add(totalLocalNumberOfFailures);
      globalNumberOfRetries.add(totalLocalNumberOfRetries);

      BasicCounters<MutableLong> aggregatedCounters = new BasicCounters<MutableLong>(MutableLong.class);
      aggregatedCounters.setCounter(AggregatedFileCounters.PROCESSED_FILES, globalProcessedFiles);
      aggregatedCounters.setCounter(AggregatedFileCounters.PENDING_FILES, pendingFiles);
      aggregatedCounters.setCounter(AggregatedFileCounters.NUMBER_OF_ERRORS, totalLocalNumberOfFailures);
      aggregatedCounters.setCounter(AggregatedFileCounters.NUMBER_OF_RETRIES, totalLocalNumberOfRetries);

      return aggregatedCounters;
    }
  }

  protected long lastRepartition = 0;
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

  /**
   * Returns the frequency with which new files are scanned for in milliseconds.
   * @return The scan interval in milliseconds.
   */
  public int getScanIntervalMillis()
  {
    return scanIntervalMillis;
  }

  /**
   * Sets the frequency with which new files are scanned for in milliseconds.
   * @param scanIntervalMillis The scan interval in milliseconds.
   */
  public void setScanIntervalMillis(int scanIntervalMillis)
  {
    if (scanIntervalMillis < 0) {
      throw new IllegalArgumentException("scanIntervalMillis should be greater than or equal to 0.");
    }
    this.scanIntervalMillis = scanIntervalMillis;
  }

  /**
   * Returns the number of tuples emitted in a batch.
   * @return The number of tuples emitted in a batch.
   */
  public int getEmitBatchSize()
  {
    return emitBatchSize;
  }

  /**
   * Sets the number of tuples to emit in a batch.
   * @param emitBatchSize The number of tuples to emit in a batch.
   */
  public void setEmitBatchSize(int emitBatchSize)
  {
    if (emitBatchSize <= 0) {
      throw new IllegalArgumentException("emitBatchSize should be greater than 0.");
    }
    this.emitBatchSize = emitBatchSize;
  }

  /**
   * Sets the idempotent storage manager on the operator.
   * @param windowDataManager  an {@link WindowDataManager}
   */
  public void setWindowDataManager(WindowDataManager windowDataManager)
  {
    this.windowDataManager = windowDataManager;
  }

  /**
   * Returns the idempotent storage manager which is being used by the operator.
   *
   * @return the idempotent storage manager.
   */
  public WindowDataManager getWindowDataManager()
  {
    return windowDataManager;
  }

  /**
   * Returns the desired number of partitions.
   * @return the desired number of partitions.
   */
  public int getPartitionCount()
  {
    return partitionCount;
  }

  /**
   * Sets the desired number of partitions.
   * @param requiredPartitions The desired number of partitions.
   */
  public void setPartitionCount(int requiredPartitions)
  {
    this.partitionCount = requiredPartitions;
  }

  /**
   * Returns the current number of partitions for the operator.
   * @return The current number of partitions for the operator.
   */
  public int getCurrentPartitions()
  {
    return currentPartitions;
  }

  @Override
  public void setup(OperatorContext context)
  {
    operatorId = context.getId();
    globalProcessedFileCount.setValue(processedFiles.size());
    LOG.debug("Setup processed file count: {}", globalProcessedFileCount);
    this.context = context;

    try {
      filePath = new Path(directory);
      configuration = new Configuration();
      fs = getFSInstance();
    } catch (IOException ex) {
      failureHandling(ex);
    }

    fileCounters.setCounter(FileCounters.GLOBAL_PROCESSED_FILES, globalProcessedFileCount);
    fileCounters.setCounter(FileCounters.LOCAL_PROCESSED_FILES, localProcessedFileCount);
    fileCounters.setCounter(FileCounters.GLOBAL_NUMBER_OF_FAILURES, globalNumberOfFailures);
    fileCounters.setCounter(FileCounters.LOCAL_NUMBER_OF_FAILURES, localNumberOfFailures);
    fileCounters.setCounter(FileCounters.GLOBAL_NUMBER_OF_RETRIES, globalNumberOfRetries);
    fileCounters.setCounter(FileCounters.LOCAL_NUMBER_OF_RETRIES, localNumberOfRetries);
    fileCounters.setCounter(FileCounters.PENDING_FILES, pendingFileCount);

    windowDataManager.setup(context);
    if (context.getValue(OperatorContext.ACTIVATION_WINDOW_ID) < windowDataManager.getLargestCompletedWindow()) {
      //reset current file and offset in case of replay
      currentFile = null;
      offset = 0;
    }
  }

  /**
   * Override this method to change the FileSystem instance that is used by the operator.
   *
   * @return A FileSystem object.
   * @throws IOException
   */
  protected FileSystem getFSInstance() throws IOException
  {
    return FileSystem.newInstance(filePath.toUri(), configuration);
  }

  @Override
  public void teardown()
  {
    IOException savedException = null;
    boolean fileFailed = false;

    try {
      if (inputStream != null) {
        inputStream.close();
      }
    } catch (IOException ex) {
      savedException = ex;
      fileFailed = true;
    }

    boolean fsFailed = false;

    try {
      fs.close();
    } catch (IOException ex) {
      savedException = ex;
      fsFailed = true;
    }

    if (savedException != null) {
      String errorMessage = "";

      if (fileFailed) {
        errorMessage += "Failed to close " + currentFile + ". ";
      }

      if (fsFailed) {
        errorMessage += "Failed to close filesystem.";
      }

      throw new RuntimeException(errorMessage, savedException);
    }
    windowDataManager.teardown();
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
    if (windowId <= windowDataManager.getLargestCompletedWindow()) {
      replay(windowId);
    }
  }

  @Override
  public void endWindow()
  {
    if (currentWindowId > windowDataManager.getLargestCompletedWindow()) {
      try {
        windowDataManager.save(currentWindowRecoveryState, currentWindowId);
      } catch (IOException e) {
        throw new RuntimeException("saving recovery", e);
      }
    }
    currentWindowRecoveryState.clear();
    if (context != null) {
      pendingFileCount.setValue(pendingFiles.size() + failedFiles.size() + unfinishedFiles.size());

      if (currentFile != null) {
        pendingFileCount.increment();
      }

      context.setCounters(fileCounters);
    }
  }

  protected void replay(long windowId)
  {
    //This operator can partition itself dynamically. When that happens a file can be re-hashed
    //to a different partition than the previous one. In order to handle this, the partition loads
    //all the recovery data for a window and then processes only those files which would be hashed
    //to it in the current run.
    try {
      Map<Integer, Object> recoveryDataPerOperator = windowDataManager.retrieveAllPartitions(windowId);

      for (Object recovery : recoveryDataPerOperator.values()) {
        @SuppressWarnings("unchecked")
        LinkedList<RecoveryEntry> recoveryData = (LinkedList<RecoveryEntry>)recovery;

        for (RecoveryEntry recoveryEntry : recoveryData) {
          if (scanner.acceptFile(recoveryEntry.file)) {
            //The operator may have continued processing the same file in multiple windows.
            //So the recovery states of subsequent windows will have an entry for that file however the offset changes.
            //In this case we continue reading from previously opened stream.
            if (currentFile == null || !(currentFile.equals(recoveryEntry.file) && offset == recoveryEntry.startOffset)) {
              if (inputStream != null) {
                closeFile(inputStream);
              }
              processedFiles.add(recoveryEntry.file);
              //removing the file from failed and unfinished queues and pending set
              Iterator<FailedFile> failedFileIterator = failedFiles.iterator();
              while (failedFileIterator.hasNext()) {
                FailedFile ff = failedFileIterator.next();
                if (ff.path.equals(recoveryEntry.file) && ff.offset == recoveryEntry.startOffset) {
                  failedFileIterator.remove();
                  break;
                }
              }

              Iterator<FailedFile> unfinishedFileIterator = unfinishedFiles.iterator();
              while (unfinishedFileIterator.hasNext()) {
                FailedFile ff = unfinishedFileIterator.next();
                if (ff.path.equals(recoveryEntry.file) && ff.offset == recoveryEntry.startOffset) {
                  unfinishedFileIterator.remove();
                  break;
                }
              }
              if (pendingFiles.contains(recoveryEntry.file)) {
                pendingFiles.remove(recoveryEntry.file);
              }
              inputStream = retryFailedFile(new FailedFile(recoveryEntry.file, recoveryEntry.startOffset));

              while (--skipCount >= 0) {
                readEntity();
              }
              while (offset < recoveryEntry.endOffset) {
                T line = readEntity();
                offset++;
                emit(line);
              }
              if (recoveryEntry.fileClosed) {
                closeFile(inputStream);
              }
            } else {
              while (offset < recoveryEntry.endOffset) {
                T line = readEntity();
                offset++;
                emit(line);
              }
              if (recoveryEntry.fileClosed) {
                closeFile(inputStream);
              }
            }
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("replay", e);
    }
  }


  @Override
  public void emitTuples()
  {
    if (currentWindowId <= windowDataManager.getLargestCompletedWindow()) {
      return;
    }

    if (inputStream == null) {
      try {
        if (currentFile != null && offset > 0) {
          //open file resets offset to 0 so this a way around it.
          long tmpOffset = offset;
          if (fs.exists(new Path(currentFile))) {
            this.inputStream = openFile(new Path(currentFile));
            offset = tmpOffset;
            skipCount = tmpOffset;
          } else {
            currentFile = null;
            offset = 0;
            skipCount = 0;
          }
        } else if (!unfinishedFiles.isEmpty()) {
          retryFailedFile(unfinishedFiles.poll());
        } else if (!pendingFiles.isEmpty()) {
          String newPathString = pendingFiles.iterator().next();
          pendingFiles.remove(newPathString);
          if (fs.exists(new Path(newPathString))) {
            this.inputStream = openFile(new Path(newPathString));
          }
        } else if (!failedFiles.isEmpty()) {
          retryFailedFile(failedFiles.poll());
        } else {
          scanDirectory();
        }
      } catch (IOException ex) {
        failureHandling(ex);
      }
    }
    if (inputStream != null) {
      long startOffset = offset;
      String file  = currentFile; //current file is reset to null when closed.
      boolean fileClosed = false;

      try {
        int counterForTuple = 0;
        while (!suspendEmit() && counterForTuple++ < emitBatchSize) {
          T line = readEntity();
          if (line == null) {
            LOG.info("done reading file ({} entries).", offset);
            closeFile(inputStream);
            fileClosed = true;
            break;
          }

          // If skipCount is non zero, then failed file recovery is going on, skipCount is
          // used to prevent already emitted records from being emitted again during recovery.
          // When failed file is open, skipCount is set to the last read offset for that file.
          //
          if (skipCount == 0) {
            offset++;
            emit(line);
          } else {
            skipCount--;
          }
        }
      } catch (IOException e) {
        failureHandling(e);
      }
      //Only when something was emitted from the file, or we have a closeFile(), then we record it for entry.
      if (offset >= startOffset) {
        currentWindowRecoveryState.add(new RecoveryEntry(file, startOffset, offset, fileClosed));
      }
    }
  }

  /**
   * Whether or not to suspend emission of tuples, regardless of emitBatchSize.
   * This is useful for subclasses to have their own logic of whether to emit during the current window.
   */
  protected boolean suspendEmit()
  {
    return false;
  }

  /**
   * Notifies that the directory is being scanned.<br>
   * Override this method to custom handling. Will be called once
   */
  protected void visitDirectory(Path filePath)
  {
  }

  private void checkVisitedDirectory(Path path)
  {
    String pathString = path.toString();
    if (!processedFiles.contains(pathString)) {
      visitDirectory(path);
      processedFiles.add(pathString);
    }
  }

  /**
   * Scans the directory for new files.
   */
  protected void scanDirectory()
  {
    if (System.currentTimeMillis() - scanIntervalMillis >= lastScanMillis) {
      Set<Path> newPaths = scanner.scan(fs, filePath, processedFiles);

      for (Path newPath : newPaths) {
        try {
          FileStatus fileStatus = fs.getFileStatus(newPath);
          if (fileStatus.isDirectory())  {
            checkVisitedDirectory(newPath);
          } else {
            String newPathString = newPath.toString();
            pendingFiles.add(newPathString);
            processedFiles.add(newPathString);
            localProcessedFileCount.increment();
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      lastScanMillis = System.currentTimeMillis();
    }
  }

  /**
   * Helper method for handling IOExceptions.
   * @param e The caught IOException.
   */
  private void failureHandling(Exception e)
  {
    localNumberOfFailures.increment();
    if (maxRetryCount <= 0) {
      throw new RuntimeException(e);
    }
    LOG.error("FS reader error", e);
    addToFailedList();
  }

  protected void addToFailedList()
  {
    FailedFile ff = new FailedFile(currentFile, offset, retryCount);

    try {
      // try to close file
      if (this.inputStream != null) {
        this.inputStream.close();
      }
    } catch (IOException e) {
      localNumberOfFailures.increment();
      LOG.error("Could not close input stream on: " + currentFile);
    }

    ff.retryCount++;
    ff.lastFailedTime = System.currentTimeMillis();
    ff.offset = this.offset;

    // Clear current file state.
    this.currentFile = null;
    this.inputStream = null;

    if (ff.retryCount > maxRetryCount) {
      return;
    }

    localNumberOfRetries.increment();
    LOG.info("adding to failed list path {} offset {} retry {}", ff.path, ff.offset, ff.retryCount);
    failedFiles.add(ff);
  }

  protected InputStream retryFailedFile(FailedFile ff)  throws IOException
  {
    LOG.info("retrying failed file {} offset {} retry {}", ff.path, ff.offset, ff.retryCount);
    String path = ff.path;
    if (!fs.exists(new Path(path))) {
      return null;
    }
    this.inputStream = openFile(new Path(path));
    this.offset = ff.offset;
    this.retryCount = ff.retryCount;
    this.skipCount = ff.offset;
    return this.inputStream;
  }

  protected InputStream openFile(Path path) throws IOException
  {
    currentFile = path.toString();
    offset = 0;
    retryCount = 0;
    skipCount = 0;
    LOG.info("opening file {}", path);
    InputStream input = fs.open(path);
    return input;
  }

  protected void closeFile(InputStream is) throws IOException
  {
    LOG.info("closing file {} offset {}", currentFile, offset);

    if (is != null) {
      is.close();
    }

    currentFile = null;
    inputStream = null;
  }

  @Override
  public Collection<Partition<AbstractFileInputOperator<T>>> definePartitions(Collection<Partition<AbstractFileInputOperator<T>>> partitions, PartitioningContext context)
  {
    lastRepartition = System.currentTimeMillis();

    int totalCount = getNewPartitionCount(partitions, context);

    LOG.debug("Computed new partitions: {}", totalCount);

    if (totalCount == partitions.size()) {
      return partitions;
    }

    AbstractFileInputOperator<T> tempOperator = partitions.iterator().next().getPartitionedInstance();

    MutableLong tempGlobalNumberOfRetries = tempOperator.globalNumberOfRetries;
    MutableLong tempGlobalNumberOfFailures = tempOperator.globalNumberOfRetries;

    /*
     * Build collective state from all instances of the operator.
     */
    Set<String> totalProcessedFiles = Sets.newHashSet();
    Set<FailedFile> currentFiles = Sets.newHashSet();
    List<DirectoryScanner> oldscanners = Lists.newLinkedList();
    List<FailedFile> totalFailedFiles = Lists.newLinkedList();
    List<String> totalPendingFiles = Lists.newLinkedList();
    Set<Integer> deletedOperators =  Sets.newHashSet();

    for (Partition<AbstractFileInputOperator<T>> partition : partitions) {
      AbstractFileInputOperator<T> oper = partition.getPartitionedInstance();
      totalProcessedFiles.addAll(oper.processedFiles);
      totalFailedFiles.addAll(oper.failedFiles);
      totalPendingFiles.addAll(oper.pendingFiles);
      currentFiles.addAll(unfinishedFiles);
      tempGlobalNumberOfRetries.add(oper.localNumberOfRetries);
      tempGlobalNumberOfFailures.add(oper.localNumberOfFailures);
      if (oper.currentFile != null) {
        currentFiles.add(new FailedFile(oper.currentFile, oper.offset));
      }
      oldscanners.add(oper.getScanner());
      deletedOperators.add(oper.operatorId);
    }

    /*
     * Create partitions of scanners, scanner's partition method will do state
     * transfer for DirectoryScanner objects.
     */
    List<DirectoryScanner> scanners = scanner.partition(totalCount, oldscanners);

    Collection<Partition<AbstractFileInputOperator<T>>> newPartitions = Lists.newArrayListWithExpectedSize(totalCount);
    List<WindowDataManager> newManagers = windowDataManager.partition(totalCount, deletedOperators);

    KryoCloneUtils<AbstractFileInputOperator<T>> cloneUtils = KryoCloneUtils.createCloneUtils(this);
    for (int i = 0; i < scanners.size(); i++) {

      @SuppressWarnings("unchecked")
      AbstractFileInputOperator<T> oper = cloneUtils.getClone();

      DirectoryScanner scn = scanners.get(i);
      oper.setScanner(scn);

      // Do state transfer for processed files.
      oper.processedFiles.addAll(totalProcessedFiles);
      oper.globalNumberOfFailures = tempGlobalNumberOfRetries;
      oper.localNumberOfFailures.setValue(0);
      oper.globalNumberOfRetries = tempGlobalNumberOfFailures;
      oper.localNumberOfRetries.setValue(0);

      /* redistribute unfinished files properly */
      oper.unfinishedFiles.clear();
      oper.currentFile = null;
      oper.offset = 0;
      Iterator<FailedFile> unfinishedIter = currentFiles.iterator();
      while (unfinishedIter.hasNext()) {
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
      while (pendingFilesIterator.hasNext()) {
        String pathString = pendingFilesIterator.next();
        if (scn.acceptFile(pathString)) {
          oper.pendingFiles.add(pathString);
          pendingFilesIterator.remove();
        }
      }
      oper.setWindowDataManager(newManagers.get(i));
      newPartitions.add(new DefaultPartition<AbstractFileInputOperator<T>>(oper));
    }

    LOG.info("definePartitions called returning {} partitions", newPartitions.size());
    return newPartitions;
  }

  protected int getNewPartitionCount(Collection<Partition<AbstractFileInputOperator<T>>> partitions, PartitioningContext context)
  {
    return DefaultPartition.getRequiredPartitionCount(context, this.partitionCount);
  }

  @Override
  public void partitioned(Map<Integer, Partition<AbstractFileInputOperator<T>>> partitions)
  {
    currentPartitions = partitions.size();
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {
  }

  @Override
  public void checkpointed(long windowId)
  {
  }

  @Override
  public void committed(long windowId)
  {
    try {
      windowDataManager.committed(windowId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Read the next item from the stream. Depending on the type of stream, this could be a byte array, line or object.
   * Upon return of null, the stream will be considered fully consumed.
   *
   * @return Depending on the type of stream an object is returned. When null is returned the stream is consumed.
   * @throws IOException
   */
  protected abstract T readEntity() throws IOException;

  /**
   * Emit the tuple on the port
   *
   * @param tuple
   */
  protected abstract void emit(T tuple);


  /**
   * Repartition is required when number of partitions are not equal to required
   * partitions.
   * @param batchedOperatorStats the stats to use when repartitioning.
   * @return Returns the stats listener response.
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

  /**
   * Returns the maximum number of times the operator will attempt to process
   * a file on which it encounters an error.
   * @return The maximum number of times the operator will attempt to process a
   * file on which it encounters an error.
   */
  public int getMaxRetryCount()
  {
    return maxRetryCount;
  }

  /**
   * Sets the maximum number of times the operator will attempt to process
   * a file on which it encounters an error.
   * @param maxRetryCount The maximum number of times the operator will attempt
   * to process a file on which it encounters an error.
   */
  public void setMaxRetryCount(int maxRetryCount)
  {
    if (maxRetryCount < 0) {
      throw new IllegalArgumentException("maxRetryCount should be greater than or equal to 0.");
    }
    this.maxRetryCount = maxRetryCount;
  }

  /**
   * The class that is used to scan for new files in the directory for the
   * AbstractFSDirectoryInputOperator.
   */
  public static class DirectoryScanner implements Serializable
  {
    private static final long serialVersionUID = 4535844463258899929L;
    private String filePatternRegexp;
    private transient Pattern regex = null;
    private boolean recursive = true;
    private int partitionIndex;
    private int partitionCount;
    protected final transient HashSet<String> ignoredFiles = new HashSet<String>();

    public String getFilePatternRegexp()
    {
      return filePatternRegexp;
    }

    public void setFilePatternRegexp(String filePatternRegexp)
    {
      this.filePatternRegexp = filePatternRegexp;
      this.regex = null;
    }

    public int getPartitionCount()
    {
      return partitionCount;
    }

    public int getPartitionIndex()
    {
      return partitionIndex;
    }

    protected Pattern getRegex()
    {
      if (this.regex == null && this.filePatternRegexp != null) {
        this.regex = Pattern.compile(this.filePatternRegexp);
      }
      return this.regex;
    }

    public LinkedHashSet<Path> scan(FileSystem fs, Path filePath, Set<String> consumedFiles)
    {
      LinkedHashSet<Path> pathSet = Sets.newLinkedHashSet();
      try {
        LOG.debug("Scanning {} with pattern {}", filePath, this.filePatternRegexp);
        if (!consumedFiles.contains(filePath.toString())) {
          if (fs.isDirectory(filePath)) {
            pathSet.add(filePath);
          }
        }
        FileStatus[] files = fs.listStatus(filePath);
        for (FileStatus status : files) {
          Path path = status.getPath();
          String filePathStr = path.toString();

          if (consumedFiles.contains(filePathStr)) {
            continue;
          }

          if (ignoredFiles.contains(filePathStr)) {
            continue;
          }

          if (status.isDirectory() ) {
            if (isRecursive()) {
              LinkedHashSet<Path> childPathSet = scan(fs, path, consumedFiles);
              pathSet.addAll(childPathSet);
            }
          } else if (acceptFile(filePathStr)) {
            LOG.debug("Found {}", filePathStr);
            pathSet.add(path);
          } else {
            // don't look at it again
            ignoredFiles.add(filePathStr);
          }
        }
      } catch (FileNotFoundException e) {
        LOG.warn("Failed to list directory {}", filePath, e);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return pathSet;
    }

    protected int getPartition(String filePathStr)
    {
      return filePathStr.hashCode();
    }

    protected boolean acceptFile(String filePathStr)
    {
      if (partitionCount > 1) {
        int i = getPartition(filePathStr);
        int mod = i % partitionCount;
        if (mod < 0) {
          mod += partitionCount;
        }
        LOG.debug("partition {} {} {} {}", partitionIndex, filePathStr, i, mod);

        if (mod != partitionIndex) {
          return false;
        }
      }
      Pattern regex = this.getRegex();
      if (regex != null) {
        String fileName = new File(filePathStr).getName();
        Matcher matcher = regex.matcher(fileName);
        if (!matcher.matches()) {
          return false;
        }
      }
      return true;
    }

    public List<DirectoryScanner> partition(int count)
    {
      ArrayList<DirectoryScanner> partitions = Lists.newArrayListWithExpectedSize(count);
      for (int i = 0; i < count; i++) {
        partitions.add(this.createPartition(i, count));
      }
      return partitions;
    }

    public List<DirectoryScanner>  partition(int count, @SuppressWarnings("unused") Collection<DirectoryScanner> scanners)
    {
      return partition(count);
    }

    protected DirectoryScanner createPartition(int partitionIndex, int partitionCount)
    {
      KryoCloneUtils<DirectoryScanner> cloneUtils = KryoCloneUtils.createCloneUtils(this);
      DirectoryScanner that = cloneUtils.getClone();
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

    protected void setPartitionIndex(int partitionIndex)
    {
      this.partitionIndex = partitionIndex;
    }

    protected void setPartitionCount(int partitionCount)
    {
      this.partitionCount = partitionCount;
    }

    /**
     * True if recursive; false otherwise.
     *
     * @param recursive true if recursive; false otherwise.
     */
    public boolean isRecursive()
    {
      return recursive;
    }

    /**
     * Sets whether scan will be recursive.
     *
     * @return true if recursive; false otherwise.
     */
    public void setRecursive(boolean recursive)
    {
      this.recursive = recursive;
    }
  }

  protected static class RecoveryEntry
  {
    final String file;
    final long startOffset;
    final long endOffset;
    final boolean fileClosed;

    @SuppressWarnings("unused")
    private RecoveryEntry()
    {
      file = null;
      startOffset = -1;
      endOffset = -1;
      fileClosed = false;
    }

    RecoveryEntry(String file, long startOffset, long endOffset, boolean fileClosed)
    {
      this.file = Preconditions.checkNotNull(file, "file");
      this.startOffset = startOffset;
      this.endOffset = endOffset;
      this.fileClosed = fileClosed;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RecoveryEntry)) {
        return false;
      }

      RecoveryEntry that = (RecoveryEntry)o;

      if (endOffset != that.endOffset) {
        return false;
      }
      if (startOffset != that.startOffset) {
        return false;
      }
      if (fileClosed != that.fileClosed) {
        return false;
      }
      return file.equals(that.file);

    }

    @Override
    public int hashCode()
    {
      int result = file.hashCode();
      result = 31 * result + (int)(startOffset & 0xFFFFFFFF);
      result = 31 * result + (int)(endOffset & 0xFFFFFFFF);
      return result;
    }
  }

  /**
   * This class is deprecated, use {@link LineByLineFileInputOperator}
   * <p>
   * This is an implementation of the {@link AbstractFileInputOperator} that outputs the lines in a file.&nbsp;
   * Each line is emitted as a separate tuple.&nbsp; It is emitted as a String.
   * </p>
   * <p>
   * The directory path where to scan and read files from should be specified using the {@link #directory} property.
   * </p>
   * @deprecated
   * @displayName File Line Input
   * @category Input
   * @tags fs, file, line, lines, input operator
   *
   */
  public static class FileLineInputOperator extends AbstractFileInputOperator<String>
  {
    public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

    protected transient BufferedReader br;

    @Override
    protected InputStream openFile(Path path) throws IOException
    {
      InputStream is = super.openFile(path);
      br = new BufferedReader(new InputStreamReader(is));
      return is;
    }

    @Override
    protected void closeFile(InputStream is) throws IOException
    {
      super.closeFile(is);
      br.close();
      br = null;
    }

    @Override
    protected String readEntity() throws IOException
    {
      return br.readLine();
    }

    @Override
    protected void emit(String tuple)
    {
      output.emit(tuple);
    }
  }
}
