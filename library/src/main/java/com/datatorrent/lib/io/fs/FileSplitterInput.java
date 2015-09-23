/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.Stateless;

import com.datatorrent.lib.io.IdempotentStorageManager;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * Input operator that scans a directory for files and splits a file into blocks.<br/>
 * The operator emits block metadata and file metadata.<br/>
 *
 * The file system/directory space should be different for different partitions of file splitter.
 * The scanning of
 *
 * @displayName File Splitter
 * @category Input
 * @tags file
 * @since 2.0.0
 */
@OperatorAnnotation(checkpointableWithinAppWindow = false)
public class FileSplitterInput extends AbstractFileSplitter implements InputOperator, Operator.CheckpointListener
{
  @NotNull
  private IdempotentStorageManager idempotentStorageManager;
  @NotNull
  protected final transient LinkedList<ScannedFileInfo> currentWindowRecoveryState;

  @NotNull
  private TimeBasedDirectoryScanner scanner;
  @NotNull
  private Map<String, Long> referenceTimes;

  private transient long sleepMillis;

  public FileSplitterInput()
  {
    super();
    currentWindowRecoveryState = Lists.newLinkedList();
    idempotentStorageManager = new IdempotentStorageManager.FSIdempotentStorageManager();
    referenceTimes = Maps.newHashMap();
    scanner = new TimeBasedDirectoryScanner();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    sleepMillis = context.getValue(Context.OperatorContext.SPIN_MILLIS);
    scanner.setup(context);
    idempotentStorageManager.setup(context);
    super.setup(context);

    long largestRecoveryWindow = idempotentStorageManager.getLargestRecoveryWindow();
    if (largestRecoveryWindow == Stateless.WINDOW_ID || context.getValue(Context.OperatorContext.ACTIVATION_WINDOW_ID) > largestRecoveryWindow) {
      scanner.startScanning(Collections.unmodifiableMap(referenceTimes));
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    if (windowId <= idempotentStorageManager.getLargestRecoveryWindow()) {
      replay(windowId);
    }
  }

  protected void replay(long windowId)
  {
    try {
      @SuppressWarnings("unchecked")
      LinkedList<ScannedFileInfo> recoveredData = (LinkedList<ScannedFileInfo>)idempotentStorageManager.load(operatorId, windowId);
      if (recoveredData == null) {
        //This could happen when there are multiple physical instances and one of them is ahead in processing windows.
        return;
      }
      if (blockMetadataIterator != null) {
        emitBlockMetadata();
      }
      for (ScannedFileInfo info : recoveredData) {
        updateReferenceTimes(info);
        FileMetadata fileMetadata = buildFileMetadata(info);
        filesMetadataOutput.emit(fileMetadata);

        blockMetadataIterator = new BlockMetadataIterator(this, fileMetadata, blockSize);
        if (!emitBlockMetadata()) {
          break;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("replay", e);
    }
    if (windowId == idempotentStorageManager.getLargestRecoveryWindow()) {
      scanner.startScanning(Collections.unmodifiableMap(referenceTimes));
    }
  }

  @Override
  public void emitTuples()
  {
    if (currentWindowId <= idempotentStorageManager.getLargestRecoveryWindow()) {
      return;
    }

    Throwable throwable;
    if ((throwable = scanner.atomicThrowable.get()) != null) {
      DTThrowable.rethrow(throwable);
    }
    if (blockMetadataIterator == null && scanner.discoveredFiles.isEmpty()) {
      try {
        Thread.sleep(sleepMillis);
      } catch (InterruptedException e) {
        throw new RuntimeException("waiting for work", e);
      }
    }
    process();
  }

  @Override
  protected FileInfo getFileInfo()
  {
    return scanner.pollFile();
  }

  @Override
  protected boolean processFileInfo(FileInfo fileInfo)
  {
    ScannedFileInfo scannedFileInfo = (ScannedFileInfo)fileInfo;
    if (scannedFileInfo ==  TimeBasedDirectoryScanner.DELIMITER) {
      return false;
    }
    currentWindowRecoveryState.add(scannedFileInfo);
    updateReferenceTimes(scannedFileInfo);
    return super.processFileInfo(fileInfo);
  }

  protected void updateReferenceTimes(ScannedFileInfo fileInfo)
  {
    referenceTimes.put(fileInfo.getFilePath(), fileInfo.modifiedTime);
  }

  @Override
  public void endWindow()
  {
    if (currentWindowId > idempotentStorageManager.getLargestRecoveryWindow()) {
      try {
        idempotentStorageManager.save(currentWindowRecoveryState, operatorId, currentWindowId);
      } catch (IOException e) {
        throw new RuntimeException("saving recovery", e);
      }
    }
    currentWindowRecoveryState.clear();
  }

  @Override
  protected long getDefaultBlockSize()
  {
    return scanner.fs.getDefaultBlockSize(new Path(scanner.files.iterator().next()));
  }

  @Override
  protected FileStatus getFileStatus(Path path) throws IOException
  {
    return scanner.fs.getFileStatus(path);
  }

  @Override
  public void checkpointed(long l)
  {
  }

  @Override
  public void committed(long l)
  {
    try {
      idempotentStorageManager.deleteUpTo(operatorId, l);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void teardown()
  {
    scanner.teardown();
  }

  public void setIdempotentStorageManager(IdempotentStorageManager idempotentStorageManager)
  {
    this.idempotentStorageManager = idempotentStorageManager;
  }

  public IdempotentStorageManager getIdempotentStorageManager()
  {
    return this.idempotentStorageManager;
  }

  public void setScanner(TimeBasedDirectoryScanner scanner)
  {
    this.scanner = scanner;
  }

  public TimeBasedDirectoryScanner getScanner()
  {
    return this.scanner;
  }

  public static class TimeBasedDirectoryScanner implements Runnable, Component<Context.OperatorContext>
  {
    private static long DEF_SCAN_INTERVAL_MILLIS = 5000;
    private static ScannedFileInfo DELIMITER = new ScannedFileInfo();

    private boolean recursive;

    private transient volatile boolean trigger;

    @NotNull
    @Size(min = 1)
    private final Set<String> files;

    @Min(0)
    private long scanIntervalMillis;
    private String filePatternRegularExp;

    protected transient long lastScanMillis;
    protected transient FileSystem fs;
    protected final transient LinkedBlockingDeque<ScannedFileInfo> discoveredFiles;
    protected final transient ExecutorService scanService;
    protected final transient AtomicReference<Throwable> atomicThrowable;

    private transient volatile boolean running;
    protected final transient HashSet<String> ignoredFiles;
    protected transient Pattern regex;
    protected transient long sleepMillis;
    protected transient Map<String, Long> referenceTimes;

    private transient ScannedFileInfo lastScannedInfo;
    private transient int numDiscoveredPerIteration;

    public TimeBasedDirectoryScanner()
    {
      recursive = true;
      scanIntervalMillis = DEF_SCAN_INTERVAL_MILLIS;
      files = Sets.newLinkedHashSet();
      scanService = Executors.newSingleThreadExecutor();
      discoveredFiles = new LinkedBlockingDeque<>();
      atomicThrowable = new AtomicReference<>();
      ignoredFiles = Sets.newHashSet();
    }

    @Override
    public void setup(Context.OperatorContext context)
    {
      sleepMillis = context.getValue(Context.OperatorContext.SPIN_MILLIS);
      if (filePatternRegularExp != null) {
        regex = Pattern.compile(filePatternRegularExp);
      }
      try {
        fs = getFSInstance();
      } catch (IOException e) {
        throw new RuntimeException("opening fs", e);
      }
    }

    protected void startScanning(Map<String, Long> referenceTimes)
    {
      this.referenceTimes = Preconditions.checkNotNull(referenceTimes);
      scanService.submit(this);
    }

    @Override
    public void teardown()
    {
      running = false;
      scanService.shutdownNow();
      try {
        fs.close();
      } catch (IOException e) {
        throw new RuntimeException("closing fs", e);
      }
    }

    protected FileSystem getFSInstance() throws IOException
    {
      return FileSystem.newInstance(new Path(files.iterator().next()).toUri(), new Configuration());
    }

    @Override
    public void run()
    {
      running = true;
      try {
        while (running) {
          if ((trigger || (System.currentTimeMillis() - scanIntervalMillis >= lastScanMillis)) &&
            (lastScannedInfo == null || referenceTimes.get(lastScannedInfo.getFilePath()) != null)) {
            trigger = false;
            lastScannedInfo = null;
            numDiscoveredPerIteration = 0;
            for (String afile : files) {
              scan(new Path(afile), null);
            }
            scanIterationComplete();
          } else {
            Thread.sleep(sleepMillis);
          }
        }
      } catch (Throwable throwable) {
        LOG.error("service", throwable);
        running = false;
        atomicThrowable.set(throwable);
        DTThrowable.rethrow(throwable);
      }
    }

    /**
     * Operations that need to be done once a scan is complete.
     */
    protected void scanIterationComplete()
    {
      LOG.debug("scan complete {} {}", lastScanMillis, numDiscoveredPerIteration);
      if (numDiscoveredPerIteration > 0) {
        discoveredFiles.add(DELIMITER);
      }
      lastScanMillis = System.currentTimeMillis();
    }

    protected void scan(@NotNull Path filePath, Path rootPath)
    {
      try {
        FileStatus parentStatus = fs.getFileStatus(filePath);
        String parentPathStr = filePath.toUri().getPath();

        LOG.debug("scan {}", parentPathStr);

        FileStatus[] childStatuses = fs.listStatus(filePath);
        for (FileStatus status : childStatuses) {
          Path childPath = status.getPath();
          ScannedFileInfo info = createScannedFileInfo(filePath, parentStatus, childPath, status, rootPath);

          if (skipFile(childPath, status.getModificationTime(), referenceTimes.get(info.getFilePath()))) {
            continue;
          }

          if (status.isDirectory()) {
            if (recursive) {
              scan(childPath, rootPath == null ? parentStatus.getPath() : rootPath);
            }
          }

          String childPathStr = childPath.toUri().getPath();
          if (ignoredFiles.contains(childPathStr)) {
            continue;
          }
          if (acceptFile(childPathStr)) {
            LOG.debug("found {}", childPathStr);
            processDiscoveredFile(info);
          } else {
            // don't look at it again
            ignoredFiles.add(childPathStr);
          }
        }
      } catch (FileNotFoundException fnf) {
        LOG.warn("Failed to list directory {}", filePath, fnf);
      } catch (IOException e) {
        throw new RuntimeException("listing files", e);
      }
    }

    protected void processDiscoveredFile(ScannedFileInfo info)
    {
      numDiscoveredPerIteration++;
      lastScannedInfo = info;
      discoveredFiles.add(info);
    }

    protected ScannedFileInfo createScannedFileInfo(Path parentPath, FileStatus parentStatus, Path childPath, @SuppressWarnings("UnusedParameters") FileStatus childStatus, Path rootPath)
    {
      ScannedFileInfo info;
      if (rootPath == null) {
        info = parentStatus.isDirectory() ?
          new ScannedFileInfo(parentPath.toUri().getPath(), childPath.getName(), parentStatus.getModificationTime()) :
          new ScannedFileInfo(null, childPath.toUri().getPath(), parentStatus.getModificationTime());
      } else {
        URI relativeChildURI = rootPath.toUri().relativize(childPath.toUri());
        info = new ScannedFileInfo(rootPath.toUri().getPath(), relativeChildURI.getPath(), parentStatus.getModificationTime());
      }
      return info;
    }

    /**
     * Skips file/directory based on their modification time.<br/>
     *
     * @param path                 file path
     * @param modificationTime     modification time
     * @param lastModificationTime last cached directory modification time
     * @return true to skip; false otherwise.
     * @throws IOException
     */
    protected static boolean skipFile(@SuppressWarnings("unused") @NotNull Path path, @NotNull Long modificationTime,
                                      Long lastModificationTime) throws IOException
    {
      return (!(lastModificationTime == null || modificationTime > lastModificationTime));
    }

    /**
     * Accepts file which match a regular pattern.
     *
     * @param filePathStr file path
     * @return true if the path matches the pattern; false otherwise;
     */
    protected boolean acceptFile(String filePathStr)
    {
      if (regex != null) {
        Matcher matcher = regex.matcher(filePathStr);
        if (!matcher.matches()) {
          return false;
        }
      }
      return true;
    }

    public FileInfo pollFile()
    {
      return discoveredFiles.poll();
    }

    protected int getNumDiscoveredPerIteration()
    {
      return numDiscoveredPerIteration;
    }

    /**
     * Gets the regular expression for file names to split.
     *
     * @return regular expression
     */
    public String getFilePatternRegularExp()
    {
      return filePatternRegularExp;
    }

    /**
     * Only files with names matching the given java regular expression are split.
     *
     * @param filePatternRegexp regular expression
     */
    public void setFilePatternRegularExp(String filePatternRegexp)
    {
      this.filePatternRegularExp = filePatternRegexp;
    }

    /**
     * A comma separated list of directories to scan. If the path is not fully qualified the default
     * file system is used. A fully qualified path can be provided to scan directories in other filesystems.
     *
     * @param files files
     */
    public void setFiles(String files)
    {
      Iterables.addAll(this.files, Splitter.on(",").omitEmptyStrings().split(files));
    }

    /**
     * Gets the files to be scanned.
     *
     * @return files to be scanned.
     */
    public String getFiles()
    {
      return Joiner.on(",").join(this.files);
    }

    /**
     * True if recursive; false otherwise.
     *
     * @param recursive true if recursive; false otherwise.
     */
    public void setRecursive(boolean recursive)
    {
      this.recursive = recursive;
    }

    /**
     * Sets whether scan will be recursive.
     *
     * @return true if recursive; false otherwise.
     */
    public boolean isRecursive()
    {
      return this.recursive;
    }

    /**
     * Sets the trigger which will initiate scan.
     *
     * @param trigger
     */
    public void setTrigger(boolean trigger)
    {
      this.trigger = trigger;
    }

    /**
     * The trigger which will initiate scan.
     *
     * @return trigger
     */
    public boolean isTrigger()
    {
      return this.trigger;
    }

    /**
     * Returns the frequency with which new files are scanned for in milliseconds.
     *
     * @return The scan interval in milliseconds.
     */
    public long getScanIntervalMillis()
    {
      return scanIntervalMillis;
    }

    /**
     * Sets the frequency with which new files are scanned for in milliseconds.
     *
     * @param scanIntervalMillis The scan interval in milliseconds.
     */
    public void setScanIntervalMillis(long scanIntervalMillis)
    {
      this.scanIntervalMillis = scanIntervalMillis;
    }
  }

  /**
   * File info created for files discovered by scanner
   */
  public static class ScannedFileInfo extends AbstractFileSplitter.FileInfo
  {
    protected final long modifiedTime;

    protected ScannedFileInfo()
    {
      super();
      modifiedTime = -1;
    }

    public ScannedFileInfo(@Nullable String directoryPath, @NotNull String relativeFilePath, long modifiedTime)
    {
      super(directoryPath, relativeFilePath);
      this.modifiedTime = modifiedTime;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(FileSplitterInput.class);
}
