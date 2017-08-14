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

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.wal.WindowDataManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.datatorrent.api.Component;
import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.Stateless;

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
public class FileSplitterInput extends AbstractFileSplitter implements InputOperator, Operator.CheckpointListener, Operator.CheckpointNotificationListener
{
  @NotNull
  private WindowDataManager windowDataManager;

  protected transient LinkedList<ScannedFileInfo> currentWindowRecoveryState;

  @Valid
  @NotNull
  private TimeBasedDirectoryScanner scanner;

  private Map<String, Map<String, Long>> referenceTimes;

  public FileSplitterInput()
  {
    super();
    windowDataManager = new WindowDataManager.NoopWindowDataManager();
    scanner = new TimeBasedDirectoryScanner();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    currentWindowRecoveryState = Lists.newLinkedList();
    if (referenceTimes == null) {
      referenceTimes = new ConcurrentHashMap<>();
    }
    scanner.setup(context);
    windowDataManager.setup(context);
    super.setup(context);

    long largestRecoveryWindow = windowDataManager.getLargestCompletedWindow();
    if (largestRecoveryWindow == Stateless.WINDOW_ID || context.getValue(Context.OperatorContext.ACTIVATION_WINDOW_ID) >
        largestRecoveryWindow) {
      scanner.startScanning(Collections.unmodifiableMap(referenceTimes));
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    if (windowId <= windowDataManager.getLargestCompletedWindow()) {
      replay(windowId);
    }
  }

  protected void replay(long windowId)
  {
    try {
      @SuppressWarnings("unchecked")
      LinkedList<ScannedFileInfo> recoveredData = (LinkedList<ScannedFileInfo>)windowDataManager.retrieve(windowId);
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
    if (windowId == windowDataManager.getLargestCompletedWindow()) {
      scanner.startScanning(Collections.unmodifiableMap(referenceTimes));
    }
  }

  @Override
  public void emitTuples()
  {
    if (currentWindowId <= windowDataManager.getLargestCompletedWindow()) {
      return;
    }

    Throwable throwable;
    if ((throwable = scanner.atomicThrowable.get()) != null) {
      Throwables.propagate(throwable);
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
    currentWindowRecoveryState.add(scannedFileInfo);
    updateReferenceTimes(scannedFileInfo);
    return super.processFileInfo(fileInfo);
  }

  protected void updateReferenceTimes(ScannedFileInfo fileInfo)
  {
    Map<String, Long> referenceTimePerInputDir;
    String referenceKey = fileInfo.getDirectoryPath() == null ? fileInfo.getFilePath() : fileInfo.getDirectoryPath();
    if ((referenceTimePerInputDir = referenceTimes.get(referenceKey)) == null) {
      referenceTimePerInputDir = new ConcurrentHashMap<>();
    }
    referenceTimePerInputDir.put(fileInfo.getFilePath(), fileInfo.modifiedTime);
    referenceTimes.put(referenceKey, referenceTimePerInputDir);
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
  public void beforeCheckpoint(long l)
  {
  }

  @Override
  public void checkpointed(long l)
  {
  }

  @Override
  public void committed(long l)
  {
    try {
      windowDataManager.committed(l);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void teardown()
  {
    scanner.teardown();
  }

  public void setWindowDataManager(WindowDataManager windowDataManager)
  {
    this.windowDataManager = windowDataManager;
  }

  public WindowDataManager getWindowDataManager()
  {
    return this.windowDataManager;
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
    private static String FILE_BEING_COPIED = "_COPYING_";

    private boolean recursive = true;

    private transient volatile boolean trigger;

    @NotNull
    @Size(min = 1)
    private final Set<String> files = new LinkedHashSet<>();

    @Min(0)
    private long scanIntervalMillis = DEF_SCAN_INTERVAL_MILLIS;
    private String filePatternRegularExp;

    private String ignoreFilePatternRegularExp;

    protected transient long lastScanMillis;
    protected transient FileSystem fs;
    protected transient LinkedBlockingDeque<ScannedFileInfo> discoveredFiles;
    protected transient ExecutorService scanService;
    protected transient AtomicReference<Throwable> atomicThrowable;

    private transient volatile boolean running;
    protected transient HashSet<String> ignoredFiles;

    protected transient Pattern regex;
    private transient Pattern ignoreRegex;

    protected transient long sleepMillis;
    protected transient Map<String, Map<String, Long>> referenceTimes;

    private transient ScannedFileInfo lastScannedInfo;
    private transient int numDiscoveredPerIteration;

    @Override
    public void setup(Context.OperatorContext context)
    {
      scanService = Executors.newSingleThreadExecutor();
      discoveredFiles = new LinkedBlockingDeque<>();
      atomicThrowable = new AtomicReference<>();
      ignoredFiles = Sets.newHashSet();
      sleepMillis = context.getValue(Context.OperatorContext.SPIN_MILLIS);
      if (filePatternRegularExp != null) {
        regex = Pattern.compile(filePatternRegularExp);
      }
      if (ignoreFilePatternRegularExp != null) {
        ignoreRegex = Pattern.compile(this.ignoreFilePatternRegularExp);
      }

      try {
        fs = getFSInstance();
      } catch (IOException e) {
        throw new RuntimeException("opening fs", e);
      }
    }

    protected void startScanning(Map<String, Map<String, Long>> referenceTimes)
    {
      this.referenceTimes = Preconditions.checkNotNull(referenceTimes);
      scanService.submit(this);
    }

    /**
     * Stop scanner
     */
    protected void stopScanning()
    {
      running = false;
    }

    @Override
    public void teardown()
    {
      stopScanning();
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
          if ((trigger || (System.currentTimeMillis() - scanIntervalMillis >= lastScanMillis)) && isIterationCompleted()) {
            trigger = false;
            lastScannedInfo = null;
            numDiscoveredPerIteration = 0;
            for (String afile : files) {
              Path filePath = new Path(afile);
              LOG.debug("Scan started for input {}", filePath);
              Map<String, Long> lastModifiedTimesForInputDir = null;
              if (fs.exists(filePath)) {
                FileStatus fileStatus = fs.getFileStatus(filePath);
                lastModifiedTimesForInputDir = referenceTimes.get(fileStatus.getPath().toUri().getPath());
              }
              scan(filePath, null, lastModifiedTimesForInputDir);
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
        Throwables.propagate(throwable);
      }
    }

    //check if scanned files of last iteration are processed by operator thread
    private boolean isIterationCompleted()
    {
      if (lastScannedInfo == null) { // first iteration started
        return true;
      }

      LOG.debug("Directory path: {} Sub-Directory or File path: {}", lastScannedInfo.getDirectoryPath(), lastScannedInfo.getFilePath());

      /*
       * As referenceTimes is now concurrentHashMap, it throws exception if key passed is null.
       * So in case where the last scanned directory is null which likely possible when
       * only file name is specified instead of directory path.
       */
      if (lastScannedInfo.getDirectoryPath() == null) {
        return true;
      }

      Map<String, Long> referenceTime = referenceTimes.get(lastScannedInfo.getDirectoryPath());
      if (referenceTime != null) {
        return referenceTime.get(lastScannedInfo.getFilePath()) != null;
      }
      return false;
    }

    /**
     * Operations that need to be done once a scan is complete.
     */
    protected void scanIterationComplete()
    {
      LOG.debug("scan complete {} {}", lastScanMillis, numDiscoveredPerIteration);
      lastScanMillis = System.currentTimeMillis();
    }

    /**
     * This is not used anywhere and should be removed. however, currently it breaks backward compatibility, so
     * just deprecating it.
     */
    @Deprecated
    protected void scan(@NotNull Path filePath, Path rootPath) throws IOException
    {
      Map<String, Long> lastModifiedTimesForInputDir;
      lastModifiedTimesForInputDir = referenceTimes.get(filePath.toUri().getPath());
      scan(filePath, rootPath, lastModifiedTimesForInputDir);
    }

    private void scan(Path filePath, Path rootPath, Map<String, Long> lastModifiedTimesForInputDir) throws IOException
    {
      FileStatus parentStatus = fs.getFileStatus(filePath);
      String parentPathStr = filePath.toUri().getPath();

      LOG.debug("scan {}", parentPathStr);

      FileStatus[] childStatuses = fs.listStatus(filePath);

      if (childStatuses.length == 0 && rootPath == null && (lastModifiedTimesForInputDir == null || lastModifiedTimesForInputDir.get(parentPathStr) == null)) { // empty input directory copy as is
        ScannedFileInfo info = new ScannedFileInfo(null, filePath.toString(), parentStatus.getModificationTime());
        processDiscoveredFile(info);
      }

      for (FileStatus childStatus : childStatuses) {
        Path childPath = childStatus.getPath();
        String childPathStr = childPath.toUri().getPath();

        if (childStatus.isDirectory() && isRecursive()) {
          addToDiscoveredFiles(rootPath, parentStatus, childStatus, lastModifiedTimesForInputDir);
          scan(childPath, rootPath == null ? parentStatus.getPath() : rootPath, lastModifiedTimesForInputDir);
        } else if (acceptFile(childPathStr)) {
          addToDiscoveredFiles(rootPath, parentStatus, childStatus, lastModifiedTimesForInputDir);
        } else {
          // don't look at it again
          ignoredFiles.add(childPathStr);
        }
      }
    }

    private void addToDiscoveredFiles(Path rootPath, FileStatus parentStatus, FileStatus childStatus,
        Map<String, Long> lastModifiedTimesForInputDir) throws IOException
    {
      Path childPath = childStatus.getPath();
      String childPathStr = childPath.toUri().getPath();
      // Directory by now is scanned forcibly. Now check for whether file/directory needs to be added to discoveredFiles.
      Long oldModificationTime = null;
      if (lastModifiedTimesForInputDir != null) {
        oldModificationTime = lastModifiedTimesForInputDir.get(childPathStr);
      }

      if (skipFile(childPath, childStatus.getModificationTime(), oldModificationTime) || // Skip dir or file if no timestamp modification
          (childStatus.isDirectory() && (oldModificationTime != null))) { // If timestamp modified but if its a directory and already present in map, then skip.
        return;
      }

      if (ignoredFiles.contains(childPathStr)) {
        return;
      }

      ScannedFileInfo info = createScannedFileInfo(parentStatus.getPath(), parentStatus, childPath, childStatus,
          rootPath);
      processDiscoveredFile(info);
    }

    protected void processDiscoveredFile(ScannedFileInfo info)
    {
      numDiscoveredPerIteration++;
      lastScannedInfo = info;
      discoveredFiles.add(info);
      LOG.debug("discovered {} {}", info.getFilePath(), info.modifiedTime);
    }

    protected ScannedFileInfo createScannedFileInfo(Path parentPath, FileStatus parentStatus, Path childPath,
        FileStatus childStatus, Path rootPath)
    {
      ScannedFileInfo info;
      if (rootPath == null) {
        info = parentStatus.isDirectory() ?
          new ScannedFileInfo(parentPath.toUri().getPath(), childPath.getName(), childStatus.getModificationTime()) :
          new ScannedFileInfo(null, childPath.toUri().getPath(), childStatus.getModificationTime());
      } else {
        URI relativeChildURI = rootPath.toUri().relativize(childPath.toUri());
        info = new ScannedFileInfo(rootPath.toUri().getPath(), relativeChildURI.getPath(),
          childStatus.getModificationTime());
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
      if (fs.getScheme().equalsIgnoreCase("hdfs") && filePathStr.endsWith(FILE_BEING_COPIED)) {
        return false;
      }
      if (regex != null) {
        Matcher matcher = regex.matcher(filePathStr);
        if (!matcher.matches()) {
          return false;
        }
      }
      if (ignoreRegex != null) {
        Matcher matcher = ignoreRegex.matcher(filePathStr);
        if (matcher.matches()) {
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
     * @return the regular expression for ignored files.
     */
    public String getIgnoreFilePatternRegularExp()
    {
      return ignoreFilePatternRegularExp;
    }

    /**
     * Sets the regular expression for files that should be ignored.
     *
     * @param ignoreFilePatternRegex regular expression for files that will be ignored.
     */
    public void setIgnoreFilePatternRegularExp(String ignoreFilePatternRegex)
    {
      this.ignoreFilePatternRegularExp = ignoreFilePatternRegex;
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

    public long getModifiedTime()
    {
      return modifiedTime;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(FileSplitterInput.class);

}
