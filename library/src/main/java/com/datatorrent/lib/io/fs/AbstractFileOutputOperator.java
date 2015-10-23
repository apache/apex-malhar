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
package com.datatorrent.lib.io.fs;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.cache.*;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;

import com.datatorrent.lib.counters.BasicCounters;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.annotation.OperatorAnnotation;

/**
 * This base implementation for a fault tolerant HDFS output operator,
 * which can handle outputting to multiple files when the output file depends on the tuple.
 * The file a tuple is output to is determined by the getFilePath method. The operator can
 * also output files to rolling mode. In rolling mode by default file names have '.#' appended to the
 * end, where # is an integer. A maximum length for files is specified and whenever the current output
 * file size exceeds the maximum length, output is rolled over to a new file whose name ends in '.#+1'.
 * <br/>
 * <br/>
 * <b>Note:</b> This operator maintains internal state in a map of files names to offsets in the endOffsets
 * field. If the user configures this operator to write to an enormous
 * number of files, there is a risk that the operator will run out of memory. In such a case the
 * user is responsible for maintaining the internal state to prevent the operator from running out
 *
 * BenchMark Results
 * -----------------
 * The operator writes 21 MB/s with the following configuration
 *
 * Container memory size=4G
 * Tuple byte size of 32
 * output to a single file
 * Application window length of 1 second
 *
 * The operator writes 25 MB/s with the following configuration
 *
 * Container memory size=4G
 * Tuple byte size of 32
 * output to a single file
 * Application window length of 30 seconds
 *
 * @displayName FS Writer
 * @category Output
 * @tags fs, file, output operator
 *
 * @param <INPUT> This is the input tuple type.
 *
 * @since 2.0.0
 */
@OperatorAnnotation(checkpointableWithinAppWindow = false)
public abstract class AbstractFileOutputOperator<INPUT> extends BaseOperator implements Operator.CheckpointListener
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractFileOutputOperator.class);

  private static final String TMP_EXTENSION = ".tmp";

  private static final int MAX_NUMBER_FILES_IN_TEARDOWN_EXCEPTION = 25;

  /**
   * Size of the copy buffer used to restore files to checkpointed state.
   */
  private static final int COPY_BUFFER_SIZE = 1024;

  /**
   * The default number of max open files.
   */
  public final static int DEFAULT_MAX_OPEN_FILES = 100;

  /**
   * Keyname to rolling file number.
   */
  protected Map<String, MutableInt> openPart;

  /**
   * Keyname to offset of the current rolling file.
   */
  protected Map<String, MutableLong> endOffsets;

  /**
   * Keyname to counts.
   */
  protected Map<String, MutableLong> counts;

  /**
   * Filename to rotation state mapping during a rotation period. Look at {@link #rotationWindows}
   */
  @NotNull
  protected Map<String, RotationState> rotationStates;

  /**
   * The path of the directory to where files are written.
   */
  @NotNull
  protected String filePath;

  /**
   * The total number of bytes written by the operator.
   */
  protected long totalBytesWritten = 0;

  /**
   * The replication level for your output files.
   */
  @Min(0)
  protected int replication = 0;

  /**
   * The maximum number of open files you can have in your streamsCache.
   */
  @Min(1)
  protected int maxOpenFiles = DEFAULT_MAX_OPEN_FILES;

  /**
   * The maximum length in bytes of a rolling file. The default value of this is Long.MAX_VALUE
   */
  @Min(1)
  protected Long maxLength = Long.MAX_VALUE;

  /**
   * The file rotation window interval.
   * The files are rotated periodically after the specified value of windows have ended. If set to 0 this feature is
   * disabled.
   */
  @Min(0)
  protected int rotationWindows = 0;

  /**
   * True if {@link #maxLength} < {@link Long#MAX_VALUE} or {@link #rotationWindows} > 0
   */
  protected transient boolean rollingFile = false;

  /**
   * The file system used to write to.
   */
  protected transient FileSystem fs;

  protected short filePermission = 0777;

  /**
   * This is the cache which holds open file streams.
   */
  protected transient LoadingCache<String, FSFilterStreamContext> streamsCache;

  /**
   * This is the operator context passed at setup.
   */
  protected transient OperatorContext context;

  /**
   * StopWatch tracking the total time the operator has spent writing bytes.
   */
  private transient long totalWritingTime;

  /**
   * File output counters.
   */
  protected final BasicCounters<MutableLong> fileCounters = new BasicCounters<MutableLong>(MutableLong.class);

  protected StreamCodec<INPUT> streamCodec;

  /**
   * Number of windows since the last rotation
   */
  private int rotationCount;

  /**
   * If a filter stream provider is set it is used to obtain the filter that will be applied to data before it is
   * stored in the file. If it null no filter is applied and data is written as is. Multiple filters can be chained
   * together by using a filter stream chain provider.
   */
  protected FilterStreamProvider filterStreamProvider;

  /**
   * When true this will write to a filename.tmp instead of filename. This is added because at times when the operator
   * gets killed the lease of the last file it was writing to is still open in hdfs. So truncating the file during
   * recovery fails. Ideally this should be false however currently it should remain true otherwise it will lead to
   * namenode instability and crash.
   */
  protected boolean alwaysWriteToTmp = true;

  private final Map<String, String> fileNameToTmpName;
  private final Map<Long, Set<String>> finalizedFiles;
  protected final Map<String, MutableInt> finalizedPart;

  protected long currentWindow;

  /**
   * The stream is expired (closed and evicted from cache) after the specified duration has passed since it was last
   * accessed by a read or write.
   * <p/>
   * https://code.google.com/p/guava-libraries/wiki/CachesExplained <br/>
   * Caches built with CacheBuilder do not perform cleanup and evict values "automatically," or instantly after a value expires, or anything of the sort.
   * Instead, it performs small amounts of maintenance during write operations, or during occasional read operations if writes are rare.<br/>
   * This isn't the most effective way but adds a little bit of optimization.
   */
  private Long expireStreamAfterAcessMillis;

  /**
   * This input port receives incoming tuples.
   */
  public final transient DefaultInputPort<INPUT> input = new DefaultInputPort<INPUT>()
  {
    @Override
    public void process(INPUT tuple)
    {
      processTuple(tuple);
    }

    @Override
    public StreamCodec<INPUT> getStreamCodec()
    {
      if (AbstractFileOutputOperator.this.streamCodec == null) {
        return super.getStreamCodec();
      }
      else {
        return streamCodec;
      }
    }
  };

  private static class RotationState {
    boolean notEmpty;
    boolean rotated;
  }

  public AbstractFileOutputOperator()
  {
    endOffsets = Maps.newHashMap();
    counts = Maps.newHashMap();
    openPart = Maps.newHashMap();
    rotationStates = Maps.newHashMap();
    fileNameToTmpName = Maps.newHashMap();
    finalizedFiles = Maps.newTreeMap();
    finalizedPart = Maps.newHashMap();
  }

  /**
   * Override this method to change the FileSystem instance that is used by the operator.
   * This method is mainly helpful for unit testing.
   * @return A FileSystem object.
   * @throws IOException
   */
  protected FileSystem getFSInstance() throws IOException
  {
    FileSystem tempFS = FileSystem.newInstance(new Path(filePath).toUri(), new Configuration());

    if(tempFS instanceof LocalFileSystem)
    {
      tempFS = ((LocalFileSystem) tempFS).getRaw();
    }

    return tempFS;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    LOG.debug("setup initiated");
    if (expireStreamAfterAcessMillis == null) {
      expireStreamAfterAcessMillis = (long)(context.getValue(OperatorContext.SPIN_MILLIS) * context.getValue(Context.DAGContext.CHECKPOINT_WINDOW_COUNT));
    }
    rollingFile = (maxLength < Long.MAX_VALUE) || (rotationWindows > 0);

    //Getting required file system instance.
    try {
      fs = getFSInstance();
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    if (replication <= 0) {
      replication = fs.getDefaultReplication(new Path(filePath));
    }

    LOG.debug("FS class {}", fs.getClass());

    //When an entry is removed from the cache, removal listener is notified and it closes the output stream.
    RemovalListener<String, FSFilterStreamContext> removalListener = new RemovalListener<String, FSFilterStreamContext>()
    {
      @Override
      public void onRemoval(@Nonnull RemovalNotification<String, FSFilterStreamContext> notification)
      {
        FSFilterStreamContext streamContext = notification.getValue();
        if (streamContext != null) {

          //FilterOutputStream filterStream = streamContext.getFilterStream();
          try {
            String filename = notification.getKey();
            String partFileName = getPartFileNamePri(filename);

            LOG.debug("closing {}", partFileName);
            long start = System.currentTimeMillis();
            streamContext.close();
            //filterStream.close();
            totalWritingTime += System.currentTimeMillis() - start;
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    };

    //Define cache
    CacheLoader<String, FSFilterStreamContext> loader = new CacheLoader<String, FSFilterStreamContext>()
    {
      @Override
      public FSFilterStreamContext load(@Nonnull String filename)
      {
        String partFileName = getPartFileNamePri(filename);
        Path originalFilePath = new Path(filePath + Path.SEPARATOR + partFileName);

        Path activeFilePath;
        if (!alwaysWriteToTmp) {
          activeFilePath = originalFilePath;
        } else {
          //MLHR-1776 : writing to tmp file
          String tmpFileName = fileNameToTmpName.get(partFileName);
          if (tmpFileName == null) {
            tmpFileName = partFileName + '.' + System.currentTimeMillis() + TMP_EXTENSION;
            fileNameToTmpName.put(partFileName, tmpFileName);
          }
          activeFilePath = new Path(filePath + Path.SEPARATOR + tmpFileName);
        }

        FSDataOutputStream fsOutput;

        boolean sawThisFileBefore = endOffsets.containsKey(filename);

        try {
          if (fs.exists(originalFilePath) || (alwaysWriteToTmp && fs.exists(activeFilePath))) {
            if(sawThisFileBefore) {
              FileStatus fileStatus = fs.getFileStatus(activeFilePath);
              MutableLong endOffset = endOffsets.get(filename);

              if (endOffset != null) {
                endOffset.setValue(fileStatus.getLen());
              }
              else {
                endOffsets.put(filename, new MutableLong(fileStatus.getLen()));
              }

              fsOutput = fs.append(activeFilePath);
              LOG.debug("appending to {}", activeFilePath);
            }
            else {
              //We never saw this file before and we don't want to append
              //If the file is rolling we need to delete all its parts.
              if(rollingFile) {
                int part = 0;

                while (true) {
                  Path seenPartFilePath = new Path(filePath + Path.SEPARATOR + getPartFileName(filename, part));
                  if (!fs.exists(seenPartFilePath)) {
                    break;
                  }

                  fs.delete(seenPartFilePath, true);
                  part = part + 1;
                }

                fsOutput = fs.create(activeFilePath, (short) replication);
              }
              else {
                //Not rolling is easy, just delete the file and create it again.
                fs.delete(activeFilePath, true);
                if(alwaysWriteToTmp){
                  //we need to delete original file if that exists
                  if(fs.exists(originalFilePath)){
                    fs.delete(originalFilePath, true);
                  }
                }
                fsOutput = fs.create(activeFilePath, (short) replication);
              }
            }
          }
          else {
            fsOutput = fs.create(activeFilePath, (short) replication);
            fs.setPermission(activeFilePath, FsPermission.createImmutable(filePermission));
          }

          LOG.info("opened {}, active {}", partFileName, activeFilePath);
          return new FSFilterStreamContext(fsOutput);
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };

    streamsCache = CacheBuilder.newBuilder().maximumSize(maxOpenFiles).expireAfterAccess(expireStreamAfterAcessMillis, TimeUnit.MILLISECONDS).removalListener(removalListener).build(loader);

    try {
      LOG.debug("File system class: {}", fs.getClass());
      LOG.debug("end-offsets {}", endOffsets);

      //Restore the files in case they were corrupted and the operator was re-deployed.
      Path writerPath = new Path(filePath);
      if (fs.exists(writerPath)) {
        for (String seenFileName : endOffsets.keySet()) {
          String seenFileNamePart = getPartFileNamePri(seenFileName);
          LOG.debug("seenFileNamePart: {}", seenFileNamePart);

          Path activeFilePath;
          if (alwaysWriteToTmp) {
            String tmpFileName = fileNameToTmpName.get(seenFileNamePart);
            activeFilePath = new Path(filePath + Path.SEPARATOR + tmpFileName);
          } else {
            activeFilePath = new Path(filePath + Path.SEPARATOR + seenFileNamePart);
          }

          if (fs.exists(activeFilePath)) {
            LOG.debug("path exists {}", activeFilePath);
            long offset = endOffsets.get(seenFileName).longValue();
            FSDataInputStream inputStream = fs.open(activeFilePath);
            FileStatus status = fs.getFileStatus(activeFilePath);

            if (status.getLen() != offset) {
              LOG.info("path corrupted {} {} {}", activeFilePath, offset, status.getLen());
              byte[] buffer = new byte[COPY_BUFFER_SIZE];
              String recoveryFileName = seenFileNamePart + '.' + System.currentTimeMillis() + TMP_EXTENSION;
              Path recoveryFilePath = new Path(filePath + Path.SEPARATOR + recoveryFileName);
              FSDataOutputStream fsOutput = fs.create(recoveryFilePath, (short) replication);

              while (inputStream.getPos() < offset) {
                long remainingBytes = offset - inputStream.getPos();
                int bytesToWrite = remainingBytes < COPY_BUFFER_SIZE ? (int) remainingBytes : COPY_BUFFER_SIZE;
                inputStream.read(buffer);
                fsOutput.write(buffer, 0, bytesToWrite);
              }

              flush(fsOutput);
              fsOutput.close();
              inputStream.close();

              FileContext fileContext = FileContext.getFileContext(fs.getUri());
              LOG.debug("active {} recovery {} ", activeFilePath, recoveryFilePath);

              if (alwaysWriteToTmp) {
                //recovery file is used as the new tmp file and we cannot delete the old tmp file because when the operator
                //is restored to an earlier check-pointed window, it will look for an older tmp.
                fileNameToTmpName.put(seenFileNamePart, recoveryFileName);
              } else {
                LOG.debug("recovery path {} actual path {} ", recoveryFilePath, status.getPath());
                fileContext.rename(recoveryFilePath, status.getPath(), Options.Rename.OVERWRITE);
              }
            } else {
              if (alwaysWriteToTmp) {
                String currentTmp = seenFileNamePart + '.' + System.currentTimeMillis() + TMP_EXTENSION;
                FSDataOutputStream outputStream = fs.create(new Path(filePath + Path.SEPARATOR + currentTmp));
                IOUtils.copy(inputStream, outputStream);
                outputStream.close();
                fileNameToTmpName.put(seenFileNamePart, currentTmp);
              }
              inputStream.close();
            }
          }
        }
      }

      if (rollingFile) {
        //delete the left over future rolling files produced from the previous crashed instance
        //of this operator.
        for(String seenFileName: endOffsets.keySet()) {
          try {
            Integer fileOpenPart = this.openPart.get(seenFileName).getValue();
            int nextPart = fileOpenPart + 1;
            String seenPartFileName;
            while (true) {
              seenPartFileName = getPartFileName(seenFileName, nextPart);
              Path activePath = null;
              if (alwaysWriteToTmp) {
                String tmpFileName = fileNameToTmpName.get(seenPartFileName);
                if (tmpFileName != null) {
                  activePath = new Path(filePath + Path.SEPARATOR + tmpFileName);
                }
              } else {
                activePath = new Path(filePath + Path.SEPARATOR + seenPartFileName);
              }
              if (activePath == null || !fs.exists(activePath)) {
                break;
              }

              fs.delete(activePath, true);
              nextPart++;
            }

            seenPartFileName = getPartFileName(seenFileName, fileOpenPart);
            Path activePath = null;
            if (alwaysWriteToTmp) {
              String tmpFileName = fileNameToTmpName.get(seenPartFileName);
              if (tmpFileName != null) {
                activePath = new Path(filePath + Path.SEPARATOR + fileNameToTmpName.get(seenPartFileName));
              }
            } else {
              activePath = new Path(filePath + Path.SEPARATOR + seenPartFileName);
            }

            if (activePath != null && fs.getFileStatus(activePath).getLen() > maxLength) {
              //Handle the case when restoring to a checkpoint where the current rolling file
              //already has a length greater than max length.
              LOG.debug("rotating file at setup.");
              rotate(seenFileName);
            }
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
          catch (ExecutionException e) {
            throw new RuntimeException(e);
          }
        }
      }

      LOG.debug("setup completed");
      LOG.debug("end-offsets {}", endOffsets);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    this.context = context;

    fileCounters.setCounter(Counters.TOTAL_BYTES_WRITTEN,
                            new MutableLong());
    fileCounters.setCounter(Counters.TOTAL_TIME_WRITING_MILLISECONDS,
                            new MutableLong());
  }

  /**
   * Requests a file to be finalized. When it is writing to a rolling file, this will
   * request for finalizing the current open part and all the prev parts which weren't requested yet.
   *
   * @param fileName name of the file; part file name in case of rotation.
   * @throws IOException
   */
  protected void requestFinalize(String fileName)
  {
    Set<String> filesPerWindow = finalizedFiles.get(currentWindow);
    if (filesPerWindow == null) {
      filesPerWindow = Sets.newHashSet();
      finalizedFiles.put(currentWindow, filesPerWindow);
    }
    if (rollingFile) {

      MutableInt part = finalizedPart.get(fileName);
      if (part == null) {
        part = new MutableInt();
        finalizedPart.put(fileName, part);
      }
      MutableInt currentOpenPart = openPart.get(fileName);

      for (int x = part.getValue() + 1; x < currentOpenPart.getValue(); x++) {
        String prevPartNotFinalized = getPartFileName(fileName, x);
        LOG.debug("request finalize {}", prevPartNotFinalized);
        filesPerWindow.add(prevPartNotFinalized);
      }
      fileName = getPartFileNamePri(fileName);
      part.setValue(currentOpenPart.getValue());
    }
    LOG.debug("request finalize {}", fileName);
    filesPerWindow.add(fileName);
  }

  @Override
  public void teardown()
  {
    List<String> fileNames = new ArrayList<String>();
    int numberOfFailures = 0;
    IOException savedException = null;

    //Close all the streams you can
    Map<String, FSFilterStreamContext> openStreams = streamsCache.asMap();
    for(String seenFileName: openStreams.keySet()) {
      //FilterOutputStream filterStream = openStreams.get(seenFileName).getFilterStream();
      FSFilterStreamContext fsFilterStreamContext = openStreams.get(seenFileName);
      try {
        long start = System.currentTimeMillis();
        //filterStream.close();
        fsFilterStreamContext.close();
        totalWritingTime += System.currentTimeMillis() - start;
      }
      catch (IOException ex) {
        //Count number of failures
        numberOfFailures++;
        //Add names of first N failed files to list
        if(fileNames.size() < MAX_NUMBER_FILES_IN_TEARDOWN_EXCEPTION) {
          fileNames.add(seenFileName);
          //save exception
          savedException = ex;
        }
      }
    }

    //Try to close the file system
    boolean fsFailed = false;

    try {
      fs.close();
    }
    catch (IOException ex) {
      //Closing file system failed
      savedException = ex;
      fsFailed = true;
    }

    //There was a failure closing resources
    if (savedException != null) {
      String errorMessage = "";

      //File system failed to close
      if(fsFailed) {
        errorMessage += "Closing the fileSystem failed. ";
      }

      //Print names of atmost first N files that failed to close
      if(!fileNames.isEmpty()) {
        errorMessage += "The following files failed closing: ";
      }

      for(String seenFileName: fileNames) {
        errorMessage += seenFileName + ", ";
      }

      if(numberOfFailures > MAX_NUMBER_FILES_IN_TEARDOWN_EXCEPTION) {
        errorMessage += (numberOfFailures - MAX_NUMBER_FILES_IN_TEARDOWN_EXCEPTION) +
                        " more files failed.";
      }

      //Fail
      throw new RuntimeException(errorMessage, savedException);
    }

    long currentTimeStamp = System.currentTimeMillis();
  }

  /**
   * This method processes received tuples.
   * Tuples are written out to the appropriate files as determined by the getFileName method.
   * If the output port is connected incoming tuples are also converted and emitted on the appropriate output port.
   * @param tuple An incoming tuple which needs to be processed.
   */
  protected void processTuple(INPUT tuple)
  {
    String fileName = getFileName(tuple);

    if (Strings.isNullOrEmpty(fileName)) {
      return;
    }

    try {
      FilterOutputStream fsOutput = streamsCache.get(fileName).getFilterStream();
      byte[] tupleBytes = getBytesForTuple(tuple);
      long start = System.currentTimeMillis();
      fsOutput.write(tupleBytes);
      totalWritingTime += System.currentTimeMillis() - start;
      totalBytesWritten += tupleBytes.length;
      MutableLong currentOffset = endOffsets.get(fileName);

      if(currentOffset == null) {
        currentOffset = new MutableLong(0);
        endOffsets.put(fileName, currentOffset);
      }

      currentOffset.add(tupleBytes.length);

      if (rotationWindows > 0) {
        getRotationState(fileName).notEmpty = true;
      }

      if (rollingFile && currentOffset.longValue() > maxLength) {
        LOG.debug("Rotating file {} {} {}", fileName, openPart.get(fileName), currentOffset.longValue());
        rotate(fileName);
      }

      MutableLong count = counts.get(fileName);
      if (count == null) {
        count = new MutableLong(0);
        counts.put(fileName, count);
      }

      count.add(1);
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    catch (ExecutionException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * This method rolls over to the next files.
   * @param fileName The file that you are rolling.
   * @throws IllegalArgumentException
   * @throws IOException
   * @throws ExecutionException
   */
  protected void rotate(String fileName) throws IllegalArgumentException,
                                                IOException,
                                                ExecutionException
  {
    requestFinalize(fileName);
    counts.remove(fileName);
    streamsCache.invalidate(fileName);
    MutableInt mi = openPart.get(fileName);
    int rotatedFileIndex = mi.getValue();
    mi.add(1);
    LOG.debug("Part file index: {}", openPart);
    endOffsets.get(fileName).setValue(0L);
    String partFileName = getPartFileName(fileName, rotatedFileIndex);
    rotateHook(partFileName);

    if (rotationWindows > 0) {
      getRotationState(fileName).rotated = true;
    }
  }

  private RotationState getRotationState(String fileName)
  {
    RotationState rotationState = rotationStates.get(fileName);
    if (rotationState == null) {
      rotationState = new RotationState();
      rotationStates.put(fileName, rotationState);
    }
    return rotationState;
  }

  /**
   * This hook is called after a rolling file part has filled up and is closed. The hook is passed
   * the name of the file part that has just completed closed.
   * @param finishedFile The name of the file part that has just completed and closed.
   */
  protected void rotateHook(@SuppressWarnings("unused") String finishedFile)
  {
    //Do nothing by default
  }

  /**
   * This method is used to force buffers to be flushed at the end of the window.
   * flush must be used on a local file system, so an if statement checks to
   * make sure that hflush is used on local file systems.
   * @param fsOutput      output stream
   * @throws IOException
   */
  protected void flush(FSDataOutputStream fsOutput) throws IOException
  {
    if(fs instanceof LocalFileSystem ||
       fs instanceof RawLocalFileSystem) {
      fsOutput.flush();
    }
    else {
      fsOutput.hflush();
    }
  }

  /**
   * Gets the current rolling file name.
   * @param fileName The base name of the files you are rolling over.
   * @return The name of the current rolling file.
   */
  protected String getPartFileNamePri(String fileName)
  {
    if (!rollingFile) {
      return fileName;
    }

    MutableInt part = openPart.get(fileName);

    if (part == null) {
      part = new MutableInt(0);
      openPart.put(fileName, part);
      LOG.debug("First file part number {}", part);
    }

    return getPartFileName(fileName,
                           part.intValue());
  }

  /**
   * This method constructs the next rolling files name based on the
   * base file name and the number in the sequence of rolling files.
   * @param fileName The base name of the file.
   * @param part The part number of the rolling file.
   * @return The rolling file name.
   */
  protected String getPartFileName(String fileName,
                                   int part)
  {
    return fileName + "." + part;
  }

  @Override
  public void beginWindow(long windowId)
  {
    try {
      Map<String, FSFilterStreamContext> openStreams = streamsCache.asMap();
      for (FSFilterStreamContext streamContext : openStreams.values()) {
        streamContext.initializeContext();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    currentWindow = windowId;
  }

    @Override
  public void endWindow()
  {
    try {
      Map<String, FSFilterStreamContext> openStreams = streamsCache.asMap();
      for (FSFilterStreamContext streamContext: openStreams.values()) {
        long start = System.currentTimeMillis();
        streamContext.finalizeContext();
        totalWritingTime += System.currentTimeMillis() - start;
        //streamContext.resetFilter();
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (rotationWindows > 0) {
      if (++rotationCount == rotationWindows) {
        rotationCount = 0;
        // Rotate the files
        Iterator<Map.Entry<String, MutableInt>> iterator = openPart.entrySet().iterator();
        while (iterator.hasNext()) {
          String filename = iterator.next().getKey();
          // Rotate the file if the following conditions are met
          // 1. The file is not already rotated during this period for other reasons such as max length is reached
          //     or rotate was explicitly called externally
          // 2. The file is not empty
          RotationState rotationState = rotationStates.get(filename);
          boolean rotate = false;
          if (rotationState != null) {
            rotate = !rotationState.rotated && rotationState.notEmpty;
            rotationState.notEmpty = false;
          }
          if (rotate) {
            try {
              rotate(filename);
            } catch (IOException e) {
              throw new RuntimeException(e);
            } catch (ExecutionException e) {
              throw new RuntimeException(e);
            }
          }
          if (rotationState != null) {
            rotationState.rotated = false;
          }
        }
      }
    }

    fileCounters.getCounter(Counters.TOTAL_TIME_WRITING_MILLISECONDS).setValue(totalWritingTime);
    fileCounters.getCounter(Counters.TOTAL_BYTES_WRITTEN).setValue(totalBytesWritten);
    context.setCounters(fileCounters);
  }

  /**
   * This method determines the file that a received tuple is written out to.
   * @param tuple The tuple which can be used to determine the output file name.
   * @return The name of the file this tuple will be written out to.
   */
  protected abstract String getFileName(INPUT tuple);

  /**
   * This method converts a tuple into bytes, so that it can be written out to a filesystem.
   * @param tuple A received tuple to be converted into bytes.
   * @return The received tuple in byte form.
   */
  protected abstract byte[] getBytesForTuple(INPUT tuple);

  /**
   * Sets the path of the working directory where files are being written.
   * @param dir The path of the working directory where files are being written.
   */
  public void setFilePath(@Nonnull String dir)
  {
    this.filePath = dir;
  }

  /**
   * Returns the path of the working directory where files are being written.
   * @return The path of the working directory where files are being written.
   */
  public String getFilePath()
  {
    return this.filePath;
  }

  /**
   * Sets the maximum length of a an output file in bytes. By default this is Long.MAX_VALUE,
   * if this is not Long.MAX_VALUE then the output operator is in rolling mode.
   * @param maxLength The maximum length of an output file in bytes, when in rolling mode.
   */
  public void setMaxLength(long maxLength)
  {
    this.maxLength = maxLength;
  }

  /**
   * Sets the maximum length of a an output file in bytes, when in rolling mode. By default
   * this is null, if this is not null then the operator is in rolling mode.
   * @return The maximum length of an output file in bytes, when in rolling mode.
   */
  public long getMaxLength()
  {
    return maxLength;
  }

  /**
   * Gets the file rotation window interval.
   * The files are rotated periodically after the specified number of windows have ended.
   * @return The number of windows
   */
  public int getRotationWindows()
  {
    return rotationWindows;
  }

  /**
   * Sets the file rotation window interval.
   * The files are rotated periodically after the specified number of windows have ended.
   * @param rotationWindows The number of windows
   */
  public void setRotationWindows(int rotationWindows)
  {
    this.rotationWindows = rotationWindows;
  }

  /**
   * Set the maximum number of files which can be written to at a time.
   * @param maxOpenFiles The maximum number of files which can be written to at a time.
   */
  public void setMaxOpenFiles(int maxOpenFiles)
  {
    this.maxOpenFiles = maxOpenFiles;
  }

  /**
   * Get the maximum number of files which can be open.
   * @return The maximum number of files which can be open.
   */
  public int getMaxOpenFiles()
  {
    return this.maxOpenFiles;
  }

   /**
   * Get the permission on the file which is being written.
   * @return filePermission
   */
  public short getFilePermission()
  {
    return filePermission;
  }

  /**
   * Set the permission on the file which is being written.
   * @param filePermission
   */
  public void setFilePermission(short filePermission)
  {
    this.filePermission = filePermission;
  }

  /**
   * Get the filter stream provider
   * @return The filter stream provider.
   */
  public FilterStreamProvider getFilterStreamProvider()
  {
    return filterStreamProvider;
  }

  /**
   * Set the filter stream provider. When a non-null provider is specified it will be used to supply the filter that
   * will be  applied to data before it is stored in the file.
   * @param filterStreamProvider The filter stream provider
   */
  public void setFilterStreamProvider(FilterStreamProvider filterStreamProvider)
  {
    this.filterStreamProvider = filterStreamProvider;
  }

  public static enum Counters
  {
    /**
     * An enum for counters representing the total number of bytes written
     * by the operator.
     */
    TOTAL_BYTES_WRITTEN,

    /**
     * An enum for counters representing the total time the operator has
     * been operational for.
     */
    TOTAL_TIME_WRITING_MILLISECONDS
  }

  private class FSFilterStreamContext implements FilterStreamContext<FilterOutputStream>
  {

    private FSDataOutputStream outputStream;

    private FilterStreamContext filterContext;
    private NonCloseableFilterOutputStream outputWrapper;

    public FSFilterStreamContext(FSDataOutputStream outputStream) throws IOException
    {
      this.outputStream = outputStream;
      outputWrapper = new NonCloseableFilterOutputStream(outputStream);
      //resetFilter();
      initializeContext();
    }

    @Override
    public FilterOutputStream getFilterStream()
    {
      if (filterContext != null) {
        return filterContext.getFilterStream();
      }
      return outputStream;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void finalizeContext() throws IOException
    {
      if (filterContext != null) {
        filterContext.finalizeContext();
        outputWrapper.flush();
      }
      outputStream.hflush();
      if (filterStreamProvider != null) {
        filterStreamProvider.reclaimFilterStreamContext(filterContext);
      }
    }

    @SuppressWarnings("unchecked")
    public void initializeContext() throws IOException
    {
      if (filterStreamProvider != null) {
        filterContext = filterStreamProvider.getFilterStreamContext(outputWrapper);
      }
    }

    public void close() throws IOException
    {
      //finalizeContext();
      if (filterContext != null) {
        filterContext.getFilterStream().close();
      }
      outputStream.close();
    }

  }

  private static class NonCloseableFilterOutputStream extends FilterOutputStream
  {
    public NonCloseableFilterOutputStream(OutputStream out)
    {
      super(out);
    }

    @Override
    public void close() throws IOException
    {
    }
  }

  @Override
  public void checkpointed(long l)
  {
  }

  @Override
  public void committed(long l)
  {
    if (alwaysWriteToTmp) {
      Iterator<Map.Entry<Long, Set<String>>> finalizedFilesIter = finalizedFiles.entrySet().iterator();
      try {

        while (finalizedFilesIter.hasNext()) {
          Map.Entry<Long, Set<String>> filesPerWindow = finalizedFilesIter.next();
          if (filesPerWindow.getKey() > l) {
            break;
          }
          for (String file : filesPerWindow.getValue()) {
            finalizeFile(file);
          }
          finalizedFilesIter.remove();
        }
      } catch (IOException e) {
        throw new RuntimeException("failed to commit", e);
      }
    }
  }

  /**
   * Finalizing a file means that the same file will never be open again.
   *
   * @param fileName name of the file to finalize
   */
  protected void finalizeFile(String fileName) throws IOException
  {
    String tmpFileName = fileNameToTmpName.get(fileName);
    FileContext fileContext = FileContext.getFileContext(fs.getUri());
    Path srcPath = new Path(filePath + Path.SEPARATOR + tmpFileName);
    Path destPath = new Path(filePath + Path.SEPARATOR + fileName);

    if (!fs.exists(destPath)) {
      LOG.debug("rename from tmp {} actual {} ", tmpFileName, fileName);
      fileContext.rename(srcPath, destPath);
    } else if (fs.exists(srcPath)) {
      //if the destination and src both exists that means there was a failure between file rename and clearing the endOffset so
      //we just delete the tmp file.
      LOG.debug("deleting tmp {}", tmpFileName);
      fs.delete(srcPath, true);
    }
    endOffsets.remove(fileName);
    fileNameToTmpName.remove(fileName);

    //when writing to tmp files there can be vagrant tmp files which we have to clean
    FileStatus[] statuses = fs.listStatus(destPath.getParent());
    for (FileStatus status : statuses) {
      String statusName = status.getPath().getName();
      if (statusName.endsWith(TMP_EXTENSION) && statusName.startsWith(destPath.getName())) {
        //a tmp file has tmp extension always preceded by timestamp
        String actualFileName = statusName.substring(0, statusName.lastIndexOf('.', statusName.lastIndexOf('.') - 1));
        if (fileName.equals(actualFileName)) {
          LOG.debug("deleting vagrant file {}", statusName);
          fs.delete(status.getPath(), true);
        }
      }
    }
  }

  /**
   * @return true if writing to a tmp file rather than the actual file. false otherwise.
   */
  public boolean isAlwaysWriteToTmp()
  {
    return alwaysWriteToTmp;
  }

  /**
   * This controls if data is always written to a tmp file rather than the actual file. Tmp files are renamed to actual
   * files when files are finalized.
   * @param alwaysWriteToTmp true if write to a tmp file; false otherwise.
   */
  public void setAlwaysWriteToTmp(boolean alwaysWriteToTmp)
  {
    this.alwaysWriteToTmp = alwaysWriteToTmp;
  }

  @VisibleForTesting
  protected Map<String, String> getFileNameToTmpName()
  {
    return fileNameToTmpName;
  }

  @VisibleForTesting
  protected Map<Long, Set<String>> getFinalizedFiles()
  {
    return finalizedFiles;
  }

  public Long getExpireStreamAfterAccessMillis()
  {
    return expireStreamAfterAcessMillis;
  }

  /**
   * Sets the duration after which the stream is expired (closed and removed from the cache) since it was last accessed
   * by a read or write.
   *
   * @param millis time in millis.
   */
  public void setExpireStreamAfterAccessMillis(Long millis)
  {
    this.expireStreamAfterAcessMillis = millis;
  }

/**
   * Return the filter to use. If this method returns a filter the filter is applied to data before the data is stored
   * in the file. If it returns null no filter is applied and data is written as is. Override this method to provide
   * the filter implementation. Multiple filters can be chained together to return a chain filter.
   *
   * @param outputStream
   * @return
   */
  /*
  protected FilterStreamContext getStreamContext(OutputStream outputStream) throws IOException
  {
    return null;
  }
  */

}
