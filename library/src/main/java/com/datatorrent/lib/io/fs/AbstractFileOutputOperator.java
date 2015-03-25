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

import java.io.FilterOutputStream;
import java.io.IOException;
<<<<<<< HEAD
import java.io.OutputStream;
=======

>>>>>>> dt-dev
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

import com.datatorrent.lib.counters.BasicCounters;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.annotation.OperatorAnnotation;
import org.apache.hadoop.fs.permission.FsPermission;

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
public abstract class AbstractFileOutputOperator<INPUT> extends BaseOperator
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
      public void onRemoval(RemovalNotification<String, FSFilterStreamContext> notification)
      {
        FSFilterStreamContext streamContext = notification.getValue();
        if (streamContext != null) {
          
          //FilterOutputStream filterStream = streamContext.getFilterStream();
          try {
            LOG.debug("closing {}", notification.getKey());
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
      public FSFilterStreamContext load(String filename)
      {
        String partFileName = getPartFileNamePri(filename);
        Path lfilepath = new Path(filePath + Path.SEPARATOR + partFileName);

        FSDataOutputStream fsOutput;

        boolean sawThisFileBefore = endOffsets.containsKey(filename);

        try {
          if (fs.exists(lfilepath)) {
            if(sawThisFileBefore) {
              FileStatus fileStatus = fs.getFileStatus(lfilepath);
              MutableLong endOffset = endOffsets.get(filename);

              if (endOffset != null) {
                endOffset.setValue(fileStatus.getLen());
              }
              else {
                endOffsets.put(filename, new MutableLong(fileStatus.getLen()));
              }

              fsOutput = fs.append(lfilepath);
              LOG.debug("appending to {}", lfilepath);
            }
            //We never saw this file before and we don't want to append
            else {
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

                fsOutput = fs.create(lfilepath, (short) replication);
              }
              //Not rolling is easy, just delete the file and create it again.
              else {
                fs.delete(lfilepath, true);
                fsOutput = fs.create(lfilepath, (short) replication);
              }
            }
          }
          else {
            fsOutput = fs.create(lfilepath, (short) replication);
            fs.setPermission(lfilepath, FsPermission.createImmutable(filePermission));
          }

          //Get the end offset of the file.

          LOG.info("opened: {}", fs.getFileStatus(lfilepath).getPath());
          return new FSFilterStreamContext(fsOutput);
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };

    streamsCache = CacheBuilder.newBuilder().maximumSize(maxOpenFiles).removalListener(removalListener).build(loader);

    try {
      LOG.debug("File system class: {}", fs.getClass());
      LOG.debug("end-offsets {}", endOffsets);

      //Restore the files in case they were corrupted and the operator
      Path writerPath = new Path(filePath);
      if (fs.exists(writerPath)) {
        for (String seenFileName: endOffsets.keySet()) {
          String seenFileNamePart = getPartFileNamePri(seenFileName);
          LOG.debug("seenFileNamePart: {}", seenFileNamePart);
          Path seenPartFilePath = new Path(filePath + Path.SEPARATOR + seenFileNamePart);
          if (fs.exists(seenPartFilePath)) {
            LOG.debug("file exists {}", seenFileNamePart);
            long offset = endOffsets.get(seenFileName).longValue();
            FSDataInputStream inputStream = fs.open(seenPartFilePath);
            FileStatus status = fs.getFileStatus(seenPartFilePath);

            if (status.getLen() != offset) {
              LOG.info("file corrupted {} {} {}", seenFileNamePart, offset, status.getLen());
              byte[] buffer = new byte[COPY_BUFFER_SIZE];

              Path tmpFilePath = new Path(filePath + Path.SEPARATOR + seenFileNamePart + TMP_EXTENSION);
              FSDataOutputStream fsOutput = fs.create(tmpFilePath, (short) replication);
              while (inputStream.getPos() < offset) {
                long remainingBytes = offset - inputStream.getPos();
                int bytesToWrite = remainingBytes < COPY_BUFFER_SIZE ? (int)remainingBytes : COPY_BUFFER_SIZE;
                inputStream.read(buffer);
                fsOutput.write(buffer, 0, bytesToWrite);
              }

              flush(fsOutput);
              fsOutput.close();
              inputStream.close();

              FileContext fileContext = FileContext.getFileContext(fs.getUri());
              LOG.debug("temp file path {}, rolling file path {}", tmpFilePath.toString(), status.getPath().toString());
              fileContext.rename(tmpFilePath, status.getPath(), Options.Rename.OVERWRITE);
            }
            else {
              inputStream.close();
            }
          }
        }
      }

      //delete the left over future rolling files produced from the previous crashed instance
      //of this operator.
      if (rollingFile) {
        for(String seenFileName: endOffsets.keySet()) {
          try {
            Integer part = openPart.get(seenFileName).getValue() + 1;

            while (true) {
              Path seenPartFilePath = new Path(filePath + Path.SEPARATOR + getPartFileName(seenFileName, part));
              if (!fs.exists(seenPartFilePath)) {
                break;
              }

              fs.delete(seenPartFilePath, true);
              part = part + 1;
            }

            Path seenPartFilePath = new Path(filePath + Path.SEPARATOR + getPartFileName(seenFileName,
                                      openPart.get(seenFileName).intValue()));

            //Handle the case when restoring to a checkpoint where the current rolling file
            //already has a length greater than max length.
            if (fs.getFileStatus(seenPartFilePath).getLen() > maxLength) {
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
        LOG.debug("Rotating file {} {}", fileName, currentOffset.longValue());
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
    counts.remove(fileName);
    streamsCache.invalidate(fileName);
    MutableInt mi = openPart.get(fileName);
    int rotatedFileIndex = mi.getValue();
    mi.add(1);
    LOG.debug("Part file index: {}", openPart);
    endOffsets.get(fileName).setValue(0L);

    rotateHook(getPartFileName(fileName, rotatedFileIndex));

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
  public void endWindow()
  {
    try {
      Map<String, FSFilterStreamContext> openStreams = streamsCache.asMap();
      for (FSFilterStreamContext streamContext: openStreams.values()) {
        long start = System.currentTimeMillis();
        streamContext.finalizeContext();
        totalWritingTime += System.currentTimeMillis() - start;
        streamContext.resetFilter();
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (rotationWindows > 0) {
      if (++rotationCount == rotationWindows) {
        rotationCount = 0;
        // Rotate the files
        Iterator<String> iterator = streamsCache.asMap().keySet().iterator();
        while (iterator.hasNext()) {
          String filename = iterator.next();
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
   * Sets the maximum length of a an output file in bytes. By default this is null,
   * if this is not null then the output operator is in rolling mode.
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

  private class FSFilterStreamContext implements FilterStreamContext<FilterOutputStream, FSDataOutputStream>
  {
    
    private FSDataOutputStream outputStream;
    
    private FilterStreamContext filterContext;
    private NonCloseableFilterOutputStream outputWrapper;
    
    public FSFilterStreamContext(FSDataOutputStream outputStream) throws IOException
    {
      setup(outputStream);
    }
    
    @Override
    public void setup(FSDataOutputStream outputStream) throws IOException
    {
      this.outputStream = outputStream;     
      outputWrapper = new NonCloseableFilterOutputStream(outputStream);
      resetFilter();
    }

    @Override
    public FilterOutputStream getFilterStream()
    {
      if (filterContext != null) {
        return filterContext.getFilterStream();
      }
      return outputStream;
    }

    @Override
    public void finalizeContext() throws IOException
    {
      if (filterContext != null) {
        filterContext.finalizeContext();
        outputWrapper.flush();
        filterContext.getFilterStream().close();
      }
      outputStream.hflush();
    }
    
    public void resetFilter() throws IOException
    {
      filterContext = getStreamContext(outputWrapper);
    }
    
    public void close() throws IOException
    {
      finalizeContext();
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

  /**
   * Return the filter to use. If this method returns a filter the filter is applied to data before the data is stored
   * in the file. If it returns null no filter is applied and data is written as is. Override this method to provide
   * the filter implementation. Multiple filters can be chained together to return a chain filter.
   *
   * @param outputStream
   * @return
   */
  protected FilterStreamContext getStreamContext(OutputStream outputStream) throws IOException
  {
    return null;
  }

}
