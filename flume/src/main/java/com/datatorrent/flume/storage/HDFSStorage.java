/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.storage;

import java.io.DataInputStream;
import java.io.IOException;

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
  private static final String identityFileName = "/counter";
  private static final String cleanFileName = "/clean-counter";
  private static final String offsetFileName = "/offsetFile";
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
  private Path offsetFile;
  private FileSystem fs;
  private FSDataOutputStream dataStream;
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
  private byte[] flushedOffset;
  private long skipOffset;

  
  /**
   * This stores the Identifier information identified in the last store function call
   */
  // private byte[] fileOffset = new byte[IDENTIFIER_SIZE];

  @Override
  public void configure(Context ctx)
  {
    Configuration conf = new Configuration();
    baseDir = ctx.getString(BASE_DIR_KEY, conf.get("hadoop.tmp.dir"));
    // offset = ctx.getInteger(OFFSET_KEY, 4);
    offset = 4;
    skipOffset = -1;

    boolean restore = ctx.getBoolean(RESTORE_KEY, true);
    if (baseDir.length() > 0) {
      try {
        fs = FileSystem.get(conf);
        Path path = new Path(baseDir);
        logger.debug("Base path {}", path);
        if (!fs.exists(path)) {
          throw new RuntimeException(String.format("baseDir passed (%s) doesn't exist.", baseDir));
        }
        if (!fs.isDirectory(path)) {
          throw new RuntimeException(String.format("baseDir passed (%s) is not a directory.", baseDir));
        }
        /* keeping the block size 2MB less than the default block size */

        blockSize = ctx.getLong(BLOCKSIZE, fs.getDefaultBlockSize(path));
        fileCounter = 0;
        cleanedFileCounter = -1;
        fileCounterFile = new Path(baseDir + identityFileName);
        cleanFileCounterFile = new Path(baseDir + cleanFileName);
        offsetFile = new Path(baseDir + offsetFileName);
        if (restore) {
          if (fs.exists(fileCounterFile) && fs.isFile(fileCounterFile)) {
            fileCounter = Long.valueOf(new String(readData(fileCounterFile)));
          }
          if (fs.exists(cleanFileCounterFile) && fs.isFile(cleanFileCounterFile)) {
            cleanedFileCounter = Long.valueOf(new String(readData(cleanFileCounterFile)));
          }
          if (fs.exists(offsetFile) && fs.isFile(offsetFile)) {
            flushedOffset = readData(offsetFile);
          }
        }else{
          flushedOffset = new byte[8];
          writeData(cleanFileCounterFile, String.valueOf(cleanedFileCounter).getBytes()).close();
        }

      } catch (IOException io) {
        throw new RuntimeException(io);
      }

    } else {
      throw new RuntimeException(String.format("baseDir (%s) can't be empty", baseDir));
    }
  }

  /**
   * This function reads the file at a location and return the bytes stored in the file
   * 
   * @param path
   *          the location of the file
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
    // 4 for the number of bytes used to store the length of the data
    if (fileWriteOffset + bytes.length + DATA_LENGTH_BYTE_SIZE < blockSize) {
      try {

        if (fileWriteOffset == 0) {
          dataStream = writeData(new Path(baseDir + "/" + String.valueOf(fileCounter)), Ints.toByteArray(bytes.length));
          dataStream.write(bytes);
          writeData(fileCounterFile, String.valueOf(fileCounter + 1).getBytes()).close();
        } else {
          dataStream.write(Ints.toByteArray(bytes.length));
          dataStream.write(bytes);
        }
        byte[] fileOffset = null;
        if(fileWriteOffset > skipOffset){
          fileOffset = new byte[IDENTIFIER_SIZE];
          Server.writeLong(fileOffset, 0, calculateOffset(fileWriteOffset, fileCounter));
        }
        
        fileWriteOffset += (bytes.length + DATA_LENGTH_BYTE_SIZE);
        return fileOffset;
      } catch (IOException ex) {
        logger.warn("Error while storing the bytes {}", ex.getMessage());
        throw new RuntimeException(ex);
      }
    }
    try {
      dataStream.close();
    } catch (IOException ex) {
      logger.warn("Error while closing the streams {}", ex.getMessage());
      throw new RuntimeException(ex);
    }
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
    if (retrievalFile > fileCounter) {
      return null;
    }
    try {
      if (readStream != null) {
        readStream.close();
      }
      readStream = new FSDataInputStream(fs.open(new Path(baseDir + "/" + retrievalFile)));
      readStream.seek(retrievalOffset);
      return retrieveHelper();
    } catch (IOException e) {
      logger.warn(" error while retrieving {}", e.getMessage());
      // this is done for failure
      if (retrievalFile == byteArrayToLong(flushedOffset, offset)) {
        try {
          retrievalFile++;
          retrievalOffset = retrievalOffset - byteArrayToLong(flushedOffset, 0);
          return retrieveFailure();
        } catch (Exception e1) {
          retrievalFile--;
          skipOffset = retrievalOffset;
          retrievalOffset = retrievalOffset + byteArrayToLong(flushedOffset, 0);
          throw new RuntimeException(e1);
        }
      }else{
        throw new RuntimeException(e);
      }
    }    
  }

  private byte[] retrieveFailure() throws Exception
  {
    if (readStream != null) {
      readStream.close();
    }
    readStream = new FSDataInputStream(fs.open(new Path(baseDir + "/" + (retrievalFile))));
    readStream.seek(retrievalOffset);
    return retrieveHelper();
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
    if (readStream.available() < 1) {
      retrievalOffset = 0;
      retrievalFile++;
    }
    Server.writeLong(data, 0, calculateOffset(retrievalOffset, retrievalFile));
    return data;
  }

  @Override
  public byte[] retrieveNext()
  {
    if (readStream == null) {
      return null;
    }
    try {
      if (readStream.available() < 1) {
        if (retrievalFile > fileCounter) {
          throw new RuntimeException("no records available");
        }
        readStream.close();
        readStream = new FSDataInputStream(fs.open(new Path(baseDir + "/" + retrievalFile)));
      }
      return retrieveHelper();
    } catch (IOException e) {
      logger.warn(" error while retrieving {}", e.getMessage());
      throw new RuntimeException(e);
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
        Path path = new Path(baseDir + "/" + cleanedFileCounter);
        if (fs.exists(path) && fs.isFile(path)) {
          fs.delete(path, false);
        }
        ++cleanedFileCounter;
      } while (cleanedFileCounter < cleanFileIndex);
      writeData(cleanFileCounterFile, String.valueOf(cleanedFileCounter).getBytes()).close();
    } catch (IOException e) {
      logger.warn("not able to close the streams {}", e.getMessage());
      throw new RuntimeException(e);
    }
  }

  @Override
  public void flush()
  {
    if (dataStream != null) {
      try {
        dataStream.hflush();
      } catch (IOException ex) {
        logger.warn("not able to close the stream {}", ex.getMessage());
        throw new RuntimeException(ex);
      }
    }
    byte[] lastStoredOffset = new byte[IDENTIFIER_SIZE];
    Server.writeLong(lastStoredOffset, 0, calculateOffset(fileWriteOffset, fileCounter));
    try {      
      writeData(offsetFile, lastStoredOffset).close();
      flushedOffset = lastStoredOffset;
    } catch (IOException e) {
      try {
        if (Long.valueOf(new String(readData(offsetFile))) == Long.valueOf(new String(lastStoredOffset))) {
          flushedOffset = lastStoredOffset;
          return;
        }
      } catch (NumberFormatException e1) {
        throw new RuntimeException(e1);
      } catch (IOException e1) {
        throw new RuntimeException(e1);
      }
    }
  }

  @Override
  public void close()
  {
    if (dataStream != null) {
      try {
        dataStream.close();
        fileWriteOffset = 0;
        ++fileCounter;

      } catch (IOException ex) {
        logger.warn("not able to close the stream {}", ex.getMessage());
        throw new RuntimeException(ex);
      }
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(HDFSStorage.class);
}
