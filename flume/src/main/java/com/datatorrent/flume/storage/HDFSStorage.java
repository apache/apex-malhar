/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.storage;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.flume.sink.Server;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

/**
 * HDFSStorage is developed to store and retrieve the data from HDFS
 * <p />
 * The properties that can be set on HDFSStorage are: <br />
 * baseDir - The base directory where the data is going to be stored <br />
 * restore - This is used to restore the application from previous failure <br />
 * blockSize - The maximum size of the each file to created. <br />
 * 
 * @author Gaurav Gupta <gaurav@datatorrent.com>
 */
public class HDFSStorage implements Storage, Configurable
{
  private static final String IDENTITY_FILE = "/counter";
  private static final String CLEAN_FILE = "/clean-counter";
  private static final String OFFSET_SUFFIX = "-offsetFile";
  private static final String CLEAN_OFFSET_FILE = "/cleanoffsetFile";
  private static final String PATH_SEPARATOR = "/";
  public static final String BASE_DIR_KEY = "baseDir";
  public static final String OFFSET_KEY = "offset";
  public static final String RESTORE_KEY = "restore";
  public static final String BLOCKSIZE = "blockSize";
  private static final int IDENTIFIER_SIZE = 8;
  private static final int DATA_LENGTH_BYTE_SIZE = 4;
  /**
   * The baseDir where the storage facility is going to create files
   */
  private String baseDir;
  /**
   * This identifies the current file number
   */
  private long fileCounter;
  /**
   * The file that stores the fileCounter information
   */
  private Path fileCounterFile;
  /**
   * This identifies the last cleaned file number
   */
  private long cleanedFileCounter;
  /**
   * The file that stores the clean file counter information
   */
  private Path cleanFileCounterFile;
  /**
   * The file that stores the clean file offset information
   */
  private Path cleanFileOffsetFile;

  private FileSystem fs;
  private FSDataOutputStream dataStream;
  List<DataBlock> files2Commit = new ArrayList<DataBlock>();
  /**
   * The block size to be used to create the storage files
   */
  private long blockSize;
  /**
   * The offset in the current opened file
   */
  private long fileWriteOffset;
  private FSDataInputStream readStream;
  private long retrievalOffset;
  private long retrievalFile;
  private int offset;
  private long flushedLong;
  private byte[] cleanedOffset = new byte[8];
  private long skipOffset;
  private long skipFile;

  /**
   * This stores the Identifier information identified in the last store function call
   * 
   * @param ctx
   */
  // private byte[] fileOffset = new byte[IDENTIFIER_SIZE];
  @Override
  public void configure(Context ctx)
  {
    String id = ctx.getString(ID);
    if (id == null) {
      throw new RuntimeException(String.format("id can't be  null."));
    }

    Configuration conf = new Configuration();
    baseDir = ctx.getString(BASE_DIR_KEY, conf.get("hadoop.tmp.dir"));
    // offset = ctx.getInteger(OFFSET_KEY, 4);
    offset = 4;
    skipOffset = -1;
    skipFile = -1;

    boolean restore = ctx.getBoolean(RESTORE_KEY, true);
    if (baseDir.length() > 0) {
      try {
        fs = FileSystem.get(conf);
        Path path = new Path(baseDir);
        if (!fs.exists(path)) {
          throw new RuntimeException(String.format("baseDir passed (%s) doesn't exist.", baseDir));
        }
        if (!fs.isDirectory(path)) {
          throw new RuntimeException(String.format("baseDir passed (%s) is not a directory.", baseDir));
        }
        baseDir = baseDir + PATH_SEPARATOR + id;
        path = new Path(baseDir);
        if (!restore) {
          fs.delete(path, true);
        }
        if (!fs.exists(path) || !fs.isDirectory(path)) {
          fs.mkdirs(path);
        }

        blockSize = ctx.getLong(BLOCKSIZE, fs.getDefaultBlockSize(path));
        fileCounter = 0;
        cleanedFileCounter = -1;
        retrievalFile = -1;
        fileCounterFile = new Path(baseDir + IDENTITY_FILE);
        cleanFileCounterFile = new Path(baseDir + CLEAN_FILE);
        cleanFileOffsetFile = new Path(baseDir + CLEAN_OFFSET_FILE);
        if (restore) {
          if (fs.exists(fileCounterFile) && fs.isFile(fileCounterFile)) {
            fileCounter = Long.valueOf(new String(readData(fileCounterFile)));
          }
          if (fs.exists(cleanFileCounterFile) && fs.isFile(cleanFileCounterFile)) {
            cleanedFileCounter = Long.valueOf(new String(readData(cleanFileCounterFile)));
          }
          if (fs.exists(cleanFileOffsetFile) && fs.isFile(cleanFileOffsetFile)) {
            cleanedOffset = readData(cleanFileOffsetFile);
          }
        }
      } catch (IOException io) {
        throw new RuntimeException(io);
      }

    } else {
      throw new RuntimeException(String.format("baseDir (%s) can't be empty", baseDir));
    }
  }

  /**
   * This function reads the file at a location and return the bytes stored in the file "
   * 
   * @param path
   *          - the location of the file
   * @return
   * @throws IOException
   */
  private byte[] readData(Path path) throws IOException
  {
    DataInputStream is = new DataInputStream(fs.open(path));
    byte[] bytes = new byte[is.available()];
    is.readFully(bytes);
    is.close();
    return bytes;
  }

  /**
   * This function writes the bytes to a file specified by the path
   * 
   * @param path
   *          the file location
   * @param data
   *          the data to be written to the file
   * @return
   * @throws IOException
   */
  private FSDataOutputStream writeData(Path path, byte[] data) throws IOException
  {
    FSDataOutputStream stream = fs.create(path);
    stream.write(data);
    return stream;
  }

  private long calculateOffset(long fileOffset, long fileCounter)
  {
    return ((fileCounter << 32) | (fileOffset & 0xffffffffl));
  }

  @Override
  public byte[] store(byte[] bytes)
  {
    if (fileWriteOffset + bytes.length + DATA_LENGTH_BYTE_SIZE < blockSize) {
      try {

        if (fileWriteOffset == 0) {
          dataStream = writeData(new Path(baseDir + PATH_SEPARATOR + fileCounter), Ints.toByteArray(bytes.length));
          dataStream.write(bytes);
          // writeData(fileCounterFile, String.valueOf(fileCounter + 1).getBytes()).close();
        } else {
          dataStream.write(Ints.toByteArray(bytes.length));
          dataStream.write(bytes);
        }
        fileWriteOffset += (bytes.length + DATA_LENGTH_BYTE_SIZE);
        byte[] fileOffset = null;
        if (fileCounter > skipFile || (fileCounter == skipFile && fileWriteOffset >= skipOffset)) {
          fileOffset = new byte[IDENTIFIER_SIZE];
          Server.writeLong(fileOffset, 0, calculateOffset(fileWriteOffset, fileCounter));
          // once the current file write offset becomes greater than skip offset, skip offset needs to be reset to -1
          // o/w for all subsequent files as well store will return null till skipoffset

        }
        return fileOffset;
      } catch (IOException ex) {
        logger.warn("Error while storing the bytes {}", ex.getMessage());
        throw new RuntimeException(ex);
      }
    }
    files2Commit.add(new DataBlock(dataStream, fileWriteOffset, new Path(baseDir + PATH_SEPARATOR + fileCounter + OFFSET_SUFFIX)));
    fileWriteOffset = 0;
    ++fileCounter;
    return store(bytes);
  }

  /**
   * 
   * @param b
   * @param size
   * @param startIndex
   * @return
   */
  private long byteArrayToLong(byte[] b, int startIndex)
  {
    final byte b1 = 0;
    return Longs.fromBytes(b1, b1, b1, b1, b[3 + startIndex], b[2 + startIndex], b[1 + startIndex], b[startIndex]);
  }

  @Override
  public byte[] retrieve(byte[] identifier)
  {
    retrievalOffset = byteArrayToLong(identifier, 0);
    retrievalFile = byteArrayToLong(identifier, offset);

    if ((retrievalFile > fileCounter) || (retrievalFile == fileCounter && retrievalOffset >= fileWriteOffset)) {
      skipFile = retrievalFile;
      skipOffset = retrievalOffset;
      return null;
    }

    // making sure that the deleted address is not requested again
    if (retrievalFile != 0 || retrievalOffset != 0) {
      if (retrievalFile < byteArrayToLong(cleanedOffset, offset) || (retrievalFile == byteArrayToLong(cleanedOffset, offset) && retrievalOffset < byteArrayToLong(cleanedOffset, 0))) {
        logger.warn("The address asked has been deleted");
        return null;
      }
    }

    // we have just started
    if (retrievalFile == 0 && retrievalOffset == 0) {
      if (fileCounter == 0 && fileWriteOffset == 0) {
        skipOffset = 0;
        return null;
      }
      retrievalFile = byteArrayToLong(cleanedOffset, offset);
      retrievalOffset = byteArrayToLong(cleanedOffset, 0);
    }

    try {
      if (readStream != null) {
        readStream.close();
      }
      if (!fs.exists(new Path(baseDir + PATH_SEPARATOR + retrievalFile))) {
        retrievalFile = -1;
        throw new RuntimeException("Not a valid address");
      }
      byte[] flushedOffset = readData(new Path(baseDir + PATH_SEPARATOR + retrievalFile + OFFSET_SUFFIX));
      flushedLong = Server.readLong(flushedOffset, 0);
      while (retrievalOffset >= flushedLong && retrievalFile < fileCounter) {
        retrievalFile++;
        retrievalOffset -= flushedLong;
        flushedOffset = readData(new Path(baseDir + PATH_SEPARATOR + retrievalFile + OFFSET_SUFFIX));
        flushedLong = Server.readLong(flushedOffset, 0);
      }

      if (retrievalFile >= fileCounter) {
        throw new RuntimeException("Not a valid address");
      }
      readStream = new FSDataInputStream(fs.open(new Path(baseDir + PATH_SEPARATOR + retrievalFile)));
      readStream.seek(retrievalOffset);
      return retrieveHelper();
    } catch (IOException e) {
      retrievalFile = -1;
      throw new RuntimeException(e);

    }
  }

  private byte[] retrieveHelper() throws IOException
  {
    int length = readStream.readInt();
    byte[] data = new byte[length + IDENTIFIER_SIZE];
    int readSize = readStream.read(data, IDENTIFIER_SIZE, length);
    if (readSize == -1) {
      throw new IOException("Invalid identifier");
    }
    retrievalOffset += length + DATA_LENGTH_BYTE_SIZE;
    if (retrievalOffset >= flushedLong) {
      Server.writeLong(data, 0, calculateOffset(0, retrievalFile+1));       
    }else{
      Server.writeLong(data, 0, calculateOffset(retrievalOffset, retrievalFile));
    }
    return data;
  }

  @Override
  public byte[] retrieveNext()
  {
    if (retrievalFile == -1) {
      throw new RuntimeException("Call retrieve first");
    }
    try {
      if (readStream == null) {
        readStream = new FSDataInputStream(fs.open(new Path(baseDir + PATH_SEPARATOR + (retrievalFile))));
        byte[] flushedOffset = readData(new Path(baseDir + PATH_SEPARATOR + retrievalFile + OFFSET_SUFFIX));
        flushedLong = Server.readLong(flushedOffset, 0);
      }
      if (retrievalOffset >= flushedLong) {
        retrievalFile++;
        retrievalOffset=0;
        if (retrievalFile >= fileCounter) {
          logger.warn("read File is greater than write file");
          return null;
        }
        readStream.close();
        readStream = new FSDataInputStream(fs.open(new Path(baseDir + PATH_SEPARATOR + retrievalFile)));
        byte[] flushedOffset = readData(new Path(baseDir + PATH_SEPARATOR + retrievalFile + OFFSET_SUFFIX));
        flushedLong = Server.readLong(flushedOffset, 0);        
      }
      readStream.seek(retrievalOffset);
      return retrieveHelper();
    } catch (IOException e) {
      logger.warn(" error while retrieving {}", e.getMessage());
      return null;
    }
  }

  @Override
  public void clean(byte[] identifier)
  {
    long cleanFileIndex = byteArrayToLong(identifier, offset);
    if (cleanedFileCounter >= cleanFileIndex) {
      return;
    }
    try {
      do {
        Path path = new Path(baseDir + PATH_SEPARATOR + cleanedFileCounter);
        if (fs.exists(path) && fs.isFile(path)) {
          fs.delete(path, false);
        }
        path = new Path(baseDir + PATH_SEPARATOR + cleanedFileCounter + OFFSET_SUFFIX);
        if (fs.exists(path) && fs.isFile(path)) {
          fs.delete(path, false);
        }
        ++cleanedFileCounter;
      } while (cleanedFileCounter < cleanFileIndex);
      writeData(cleanFileCounterFile, String.valueOf(cleanedFileCounter).getBytes()).close();
      writeData(cleanFileOffsetFile, identifier).close();
    } catch (IOException e) {
      logger.warn("not able to close the streams {}", e.getMessage());
      throw new RuntimeException(e);
    }
  }

  /**
   * This is used mainly for cleaning up of counter files created
   */
  public void cleanHelperFiles()
  {
    try {
      fs.delete(new Path(baseDir), true);
    } catch (IOException e) {
      logger.warn(e.getMessage());
    }
  }

  @Override
  public void flush()
  {
    if (dataStream != null) {
      try {
        dataStream.hflush();
        writeData(fileCounterFile, String.valueOf(fileCounter + 1).getBytes()).close();
        updateFlushedOffset(new Path(baseDir + PATH_SEPARATOR + fileCounter+OFFSET_SUFFIX), fileWriteOffset);
      } catch (IOException ex) {
        logger.warn("not able to close the stream {}", ex.getMessage());
        throw new RuntimeException(ex);
      }
    }

    Iterator<DataBlock> itr = files2Commit.iterator();
    while (itr.hasNext()) {
      itr.next().close();
    }
    files2Commit.clear();
  }

  /**
   * This updates the flushed offset
   */
  private void updateFlushedOffset(Path file, long bytesWritten)
  {
    byte[] lastStoredOffset = new byte[IDENTIFIER_SIZE];
    Server.writeLong(lastStoredOffset, 0, bytesWritten);
    try {
      writeData(file, lastStoredOffset).close();
    } catch (IOException e) {
      try {
        if (Long.valueOf(new String(readData(file))) == Long.valueOf(new String(lastStoredOffset))) {
        }
      } catch (NumberFormatException e1) {
        throw new RuntimeException(e1);
      } catch (IOException e1) {
        throw new RuntimeException(e1);
      }
    }
  }

  class DataBlock
  {
    private FSDataOutputStream dataStream;
    private long dataOffset;
    private Path flushedData;

    public DataBlock(FSDataOutputStream stream, long bytesWritten, Path path2FlushedData)
    {
      this.dataStream = stream;
      this.dataOffset = bytesWritten;
      this.flushedData = path2FlushedData;
    }

    public void close()
    {
      if (dataStream != null) {
        try {
          dataStream.close();
          updateFlushedOffset(flushedData, dataOffset);

        } catch (IOException ex) {
          logger.warn("not able to close the stream {}", ex.getMessage());
          throw new RuntimeException(ex);
        }
      }
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(HDFSStorage.class);
}
