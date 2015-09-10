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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.OperatorAnnotation;

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
public class FileSplitterInput extends AbstractFileSplitter<FileSplitterInput.TimeBasedDirectoryScanner> implements InputOperator,
  Operator.CheckpointListener
{
  @NotNull
  protected IdempotentStorageManager idempotentStorageManager;

  @NotNull
  protected final transient LinkedList<FileInfo> currentWindowRecoveryState;

  public FileSplitterInput()
  {
    super();
    currentWindowRecoveryState = Lists.newLinkedList();
    scanner = new TimeBasedDirectoryScanner();
    idempotentStorageManager = new IdempotentStorageManager.FSIdempotentStorageManager();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    Preconditions.checkArgument(!scanner.files.isEmpty(), "empty files");
    super.setup(context);
    idempotentStorageManager.setup(context);

    if (context.getValue(Context.OperatorContext.ACTIVATION_WINDOW_ID) < idempotentStorageManager.getLargestRecoveryWindow()) {
      blockMetadataIterator = null;
    } else {
      scanner.startScanning();
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
      LinkedList<FileInfo> recoveredData = (LinkedList<FileInfo>) idempotentStorageManager.load(operatorId,
        windowId);
      if (recoveredData == null) {
        //This could happen when there are multiple physical instances and one of them is ahead in processing windows.
        return;
      }
      if (blockMetadataIterator != null) {
        emitBlockMetadata();
      }
      for (FileInfo info : recoveredData) {
        if (info.directoryPath != null) {
          scanner.lastModifiedTimes.put(info.directoryPath, info.modifiedTime);
        } else { //no directory
          scanner.lastModifiedTimes.put(info.relativeFilePath, info.modifiedTime);
        }
        FileMetadata fileMetadata = buildFileMetadata(info);
        fileCounters.getCounter(Counters.PROCESSED_FILES).increment();
        filesMetadataOutput.emit(fileMetadata);
        blockMetadataIterator = new BlockMetadataIterator((AbstractFileSplitter) this, fileMetadata, blockSize);

        if (!emitBlockMetadata()) {
          break;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("replay", e);
    }
    if (windowId == idempotentStorageManager.getLargestRecoveryWindow()) {
      scanner.startScanning();
    }
  }

  @Override
  protected boolean process(FileInfo fileInfo)
  {
    currentWindowRecoveryState.add(fileInfo);
    return super.process(fileInfo);
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
    if (blockMetadataIterator != null && blockCount < blocksThreshold) {
      emitBlockMetadata();
    }

    FileInfo fileInfo;
    while (blockCount < blocksThreshold && (fileInfo = scanner.pollFile()) != null) {
      if (!process(fileInfo)) {
        break;
      }
    }
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
    super.endWindow();
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

  public void setIdempotentStorageManager(IdempotentStorageManager idempotentStorageManager)
  {
    this.idempotentStorageManager = idempotentStorageManager;
  }

  public IdempotentStorageManager getIdempotentStorageManager()
  {
    return this.idempotentStorageManager;
  }

  public static class TimeBasedDirectoryScanner implements Runnable, AbstractFileSplitter.Scanner
  {
    private static long DEF_SCAN_INTERVAL_MILLIS = 5000;

    protected boolean recursive;

    protected transient volatile boolean trigger;

    @NotNull
    @FieldSerializer.Bind(ModifiedTimesSerializer.class)
    protected final Map<String, Long> lastModifiedTimes;

    @NotNull
    protected final Set<String> files;

    @Min(0)
    protected long scanIntervalMillis;

    private String filePatternRegularExp;

    protected transient long lastScanMillis;
    protected transient FileSystem fs;
    protected final transient LinkedBlockingDeque<FileInfo> discoveredFiles;
    protected final transient ExecutorService scanService;
    protected final transient AtomicReference<Throwable> atomicThrowable;

    private transient volatile boolean running;
    protected final transient HashSet<String> ignoredFiles;
    protected transient Pattern regex;
    protected transient long sleepMillis;

    private final transient Lock lock;

    public TimeBasedDirectoryScanner()
    {
      lastModifiedTimes = Maps.newHashMap();
      recursive = true;
      scanIntervalMillis = DEF_SCAN_INTERVAL_MILLIS;
      files = Sets.newLinkedHashSet();
      scanService = Executors.newSingleThreadExecutor();
      discoveredFiles = new LinkedBlockingDeque<>();
      atomicThrowable = new AtomicReference<>();
      ignoredFiles = Sets.newHashSet();
      lock = new Lock();
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

    public void startScanning()
    {
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
          if (trigger || (System.currentTimeMillis() - scanIntervalMillis >= lastScanMillis)) {
            synchronized (lock) {  //pauses this thread while kryo is serializing lastModifiedTimes
              trigger = false;
              for (String afile : files) {
                scan(new Path(afile), null);
              }
              scanComplete();
            }
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
    protected void scanComplete()
    {
      LOG.debug("scan complete {}", lastScanMillis);
      FileInfo fileInfo = discoveredFiles.peekLast();
      if (fileInfo != null) {
        fileInfo.lastFileOfScan = true;
      }
      lastScanMillis = System.currentTimeMillis();
    }

    protected void scan(@NotNull Path filePath, Path rootPath)
    {
      try {
        FileStatus parentStatus = fs.getFileStatus(filePath);
        String parentPathStr = filePath.toUri().getPath();

        LOG.debug("scan {}", parentPathStr);
        Long oldModificationTime = lastModifiedTimes.get(parentPathStr);
        lastModifiedTimes.put(parentPathStr, parentStatus.getModificationTime());

        if (skipFile(filePath, parentStatus.getModificationTime(), oldModificationTime)) {
          return;
        }

        LOG.debug("scan {}", filePath.toUri().getPath());

        FileStatus[] childStatuses = fs.listStatus(filePath);

        for (FileStatus status : childStatuses) {
          Path childPath = status.getPath();
          String childPathStr = childPath.toUri().getPath();

          if (skipFile(childPath, status.getModificationTime(), oldModificationTime)) {
            continue;
          }

          if (status.isDirectory()) {
            if (recursive) {
              scan(childPath, rootPath == null ? parentStatus.getPath() : rootPath);
            }
            //a directory is treated like any other discovered file.
          }

          if (ignoredFiles.contains(childPathStr)) {
            continue;
          }

          if (acceptFile(childPathStr)) {
            LOG.debug("found {}", childPathStr);

            FileInfo info;
            if (rootPath == null) {
              info = parentStatus.isDirectory() ?
                new FileInfo(parentPathStr, childPath.getName(), parentStatus.getModificationTime()) :
                new FileInfo(null, childPathStr, parentStatus.getModificationTime());
            } else {
              URI relativeChildURI = rootPath.toUri().relativize(childPath.toUri());
              info = new FileInfo(rootPath.toUri().getPath(), relativeChildURI.getPath(),
                parentStatus.getModificationTime());
            }

            discoveredFiles.add(info);
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

    /**
     * Skips file/directory based on their modification time.<br/>
     *
     * @param path                 file path
     * @param modificationTime     modification time
     * @param lastModificationTime last cached directory modification time
     * @return true to skip; false otherwise.
     * @throws IOException
     */
    protected boolean skipFile(@SuppressWarnings("unused") @NotNull Path path, @NotNull Long modificationTime,
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

    private static class Lock
    {
    }

    private class ModifiedTimesSerializer extends MapSerializer
    {
      @Override
      public void write(Kryo kryo, Output output, Map map)
      {
        synchronized (lock) {
          super.write(kryo, output, map);
        }
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(FileSplitterInput.class);
}
