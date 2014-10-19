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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.cache.*;
import com.google.common.collect.Maps;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

import com.datatorrent.lib.counters.BasicCounters;

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
 * @param <INPUT> This is the input tuple type.
 * @param <OUTPUT> This is the output tuple type.
 */
public abstract class AbstractFSWriter<INPUT, OUTPUT> extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractFSWriter.class);

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
   * False if you want to overwrite files which are encountered when the app starts.
   * True if you want to append to existing files when the app starts.
   */
  protected boolean append = true;

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
   * True if {@link #maxLength} < {@link Long#MAX_VALUE}
   */
  protected transient boolean rollingFile = false;

  /**
   * The file system used to write to.
   */
  protected transient FileSystem fs;

  /**
   * This is the cache which holds open file streams.
   */
  protected transient LoadingCache<String, FSDataOutputStream> streamsCache;

  /**
   * This is the operator context passed at setup.
   */
  private transient OperatorContext context;

  /**
   * Last time stamp collected.
   */
  private long lastTimeStamp;

  /**
   * The total time in milliseconds the operator has been running for.
   */
  private long totalTime;

  /**
   * File output counters.
   */
  private BasicCounters<MutableLong> fileCounters = new BasicCounters<MutableLong>(MutableLong.class);

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
  };

  /**
   * This output port is optional and emits converted tuples.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<OUTPUT> output = new DefaultOutputPort<OUTPUT>();

  public AbstractFSWriter()
  {
    endOffsets = Maps.newHashMap();
    counts = Maps.newHashMap();
    openPart = Maps.newHashMap();
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
    rollingFile = maxLength < Long.MAX_VALUE;

    //Getting required file system instance.
    try {
      fs = getFSInstance();
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    LOG.debug("FS class {}", fs.getClass());

    //Setting listener for debugging
    LOG.debug("setup initiated");
    RemovalListener<String, FSDataOutputStream> removalListener = new RemovalListener<String, FSDataOutputStream>()
    {
      @Override
      public void onRemoval(RemovalNotification<String, FSDataOutputStream> notification)
      {
        FSDataOutputStream value = notification.getValue();
        if (value != null) {
          try {
            LOG.debug("closing {}", notification.getKey());
            value.close();
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    };

    //Define cache
    CacheLoader<String, FSDataOutputStream> loader = new CacheLoader<String, FSDataOutputStream>()
    {
      @Override
      public FSDataOutputStream load(String filename)
      {
        String partFileName = getPartFileNamePri(filename);
        Path lfilepath = new Path(filePath + File.separator + partFileName);

        FSDataOutputStream fsOutput;
        if (replication <= 0) {
          replication = fs.getDefaultReplication(lfilepath);
        }

        boolean sawThisFileBefore = endOffsets.containsKey(filename);

        try {
          if (fs.exists(lfilepath)) {
            if(sawThisFileBefore || append) {
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
                  Path seenPartFilePath = new Path(filePath + "/"
                          + getPartFileName(filename, part));
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
          }

          //Get the end offset of the file.

          LOG.debug("full path: {}", fs.getFileStatus(lfilepath).getPath());
          return fsOutput;
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
          Path seenPartFilePath = new Path(filePath + "/" + seenFileNamePart);
          if (fs.exists(seenPartFilePath)) {
            LOG.debug("file exists {}", seenFileNamePart);
            long offset = endOffsets.get(seenFileName).longValue();
            FSDataInputStream inputStream = fs.open(seenPartFilePath);
            FileStatus status = fs.getFileStatus(seenPartFilePath);

            if (status.getLen() != offset) {
              LOG.info("file corrupted {} {} {}", seenFileNamePart, offset, status.getLen());
              byte[] buffer = new byte[COPY_BUFFER_SIZE];

              String tmpFileName = seenFileNamePart + TMP_EXTENSION;
              FSDataOutputStream fsOutput = streamsCache.get(tmpFileName);
              while (inputStream.getPos() < offset) {
                long remainingBytes = offset - inputStream.getPos();
                int bytesToWrite = remainingBytes < COPY_BUFFER_SIZE ? (int)remainingBytes : COPY_BUFFER_SIZE;
                inputStream.read(buffer);
                fsOutput.write(buffer, 0, bytesToWrite);
              }

              flush(fsOutput);
              FileContext fileContext = FileContext.getFileContext(fs.getUri());
              String tempTmpFilePath = getPartFileNamePri(filePath + File.separator + tmpFileName);

              Path tmpFilePath = new Path(tempTmpFilePath);
              tmpFilePath = fs.getFileStatus(tmpFilePath).getPath();
              LOG.debug("temp file path {}, rolling file path {}",
                        tmpFilePath.toString(),
                        status.getPath().toString());
              fileContext.rename(tmpFilePath,
                                 status.getPath(),
                                 Options.Rename.OVERWRITE);
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
              Path seenPartFilePath = new Path(filePath + "/"
                      + getPartFileName(seenFileName, part));
              if (!fs.exists(seenPartFilePath)) {
                break;
              }

              fs.delete(seenPartFilePath, true);
              part = part + 1;
            }

            Path seenPartFilePath = new Path(filePath + "/"
                    + getPartFileName(seenFileName,
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
    catch (ExecutionException e) {
      throw new RuntimeException(e);
    }

    this.context = context;
    lastTimeStamp = System.currentTimeMillis();

    fileCounters.setCounter(Counters.TOTAL_BYTES_WRITTEN,
                            new MutableLong());
    fileCounters.setCounter(Counters.TOTAL_TIME_ELAPSED,
                            new MutableLong());
  }

  @Override
  public void teardown()
  {
    ConcurrentMap<String, FSDataOutputStream> fileMap = streamsCache.asMap();
    List<String> fileNames = new ArrayList<String>();
    int numberOfFailures = 0;
    IOException savedException = null;

    //Close all the streams you can
    for(String seenFileName: streamsCache.asMap().keySet()) {
      FSDataOutputStream outputStream = fileMap.get(seenFileName);

      try {
        outputStream.close();
      }
      catch (IOException ex) {
        //Count number of failures
        numberOfFailures++;
        //Add names of first N failed files to list
        if(fileNames.size() < MAX_NUMBER_FILES_IN_TEARDOWN_EXCEPTION) {
          fileNames.add(seenFileName);
          //save excpetion
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
    totalTime += currentTimeStamp - lastTimeStamp;
    lastTimeStamp = currentTimeStamp;
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

    LOG.debug("file {}, hash {}, filecount {}",
              fileName,
              fileName.hashCode(),
              this.openPart.get(fileName));

    try {
      LOG.debug("end-offsets {}", endOffsets);

      FSDataOutputStream fsOutput = streamsCache.get(fileName);
      byte[] tupleBytes = getBytesForTuple(tuple);
      fsOutput.write(tupleBytes);
      totalBytesWritten += tupleBytes.length;
      MutableLong currentOffset = endOffsets.get(fileName);

      if(currentOffset == null) {
        currentOffset = new MutableLong(0);
        endOffsets.put(fileName,
                       currentOffset);
      }

      currentOffset.add(tupleBytes.length);

      LOG.debug("end-offsets {}", endOffsets);
      LOG.debug("tuple: {}", tuple.toString());
      LOG.debug("current position {}, max length {}", currentOffset.longValue(), maxLength);

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

      LOG.debug("count of {} =  {}", fileName, count);
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    catch (ExecutionException ex) {
      throw new RuntimeException(ex);
    }

    if (output.isConnected()) {
      output.emit(convert(tuple));
    }
  }

  /**
   * This method rolls over to the next files.
   * @param fileName The file that you are rolling.
   * @throws IllegalArgumentException
   * @throws IOException
   * @throws ExecutionException
   */
  private void rotate(String fileName) throws IllegalArgumentException,
                                                       IOException,
                                                       ExecutionException
  {
    counts.remove(fileName);
    streamsCache.invalidate(fileName);
    MutableInt mi = openPart.get(fileName);
    mi.add(1);
    LOG.debug("Part file index: {}", openPart);
    endOffsets.get(fileName).setValue(0L);
  }

  /**
   * This method is used to force buffers to be flushed at the end of the window.
   * flush must be used on a local file system, so an if statement checks to
   * make sure that hflush is used on local file systems.
   * @param fsOutput
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
  private String getPartFileNamePri(String fileName)
  {
    if (!rollingFile) {
      return fileName;
    }

    MutableInt part = openPart.get(fileName);

    if (part == null) {
      //If in append mode find the last rolling file to append to.
      if(append) {
        int partCounter;

        //Find the last existing rolling file.
        for(partCounter = -1;;
            partCounter++) {
          String partFileName = getPartFileName(fileName, partCounter + 1);
          Path lfilepath = new Path(filePath + File.separator + partFileName);

          try {
            if(!fs.exists(lfilepath)) {
              break;
            }
          }
          catch (IOException ex) {
            throw new RuntimeException(ex);
          }
        }

        //If there was no rolling file set the part number to zero.
        if(partCounter == -1) {
          partCounter = 0;
        }

        String partFileName = getPartFileName(fileName, partCounter);
        Path lfilepath = new Path(filePath + File.separator + partFileName);

        //Make sure the last rolling file does not exceed the maximum length,
        //if it does move on to the next rolling file.
        try {
          if(fs.exists(lfilepath) &&
             fs.getFileStatus(lfilepath).getLen() > maxLength) {
            partCounter++;
          }
        }
        catch (IOException ex) {
          throw new RuntimeException(ex);
        }

        part = new MutableInt(partCounter);
        openPart.put(fileName, part);
      }
      //If you are not in append mode then the first rolling file should have
      //a part number of zero.
      else {
        part = new MutableInt(0);
        openPart.put(fileName, part);
      }

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

  /**
   * This method converts incoming tuples to another type, which is then written to an output port.
   * By default this method throws an UnsupportedOperationException and must be overridden. This
   * method is only called when the output port is connected.
   * @param tuple A tuple that was received on the operators output port.
   * @return An incoming tuple which was converted to another type and is ready to be emitted on the
   * operator's output port.
   */
  protected OUTPUT convert(INPUT tuple)
  {
    throw new UnsupportedOperationException("Since the output port is connected, this method needs to be overriden.");
  }

  @Override
  public void endWindow()
  {
    for (String fileName : streamsCache.asMap().keySet()) {
      try
      {
        FSDataOutputStream fsOutput = streamsCache.get(fileName);
        fsOutput.hflush();
      }
      catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    long currentTimeStamp = System.currentTimeMillis();
    totalTime += currentTimeStamp - lastTimeStamp;
    lastTimeStamp = currentTimeStamp;

    fileCounters.getCounter(Counters.TOTAL_TIME_ELAPSED).setValue(totalTime);
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
   * This method determines whether or not the operator runs in append mode.
   * @param append If the operator is in append mode then previously existing files with the same name will be appended to
   */
  public void setAppend(boolean append)
  {
    this.append = append;
  }

  /**
   * Returns whether or not this operator is running in append mode.
   * @return True if the operator is appending. False otherwise.
   */
  public boolean isAppend()
  {
    return this.append;
  }

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
    TOTAL_TIME_ELAPSED
  }
}
