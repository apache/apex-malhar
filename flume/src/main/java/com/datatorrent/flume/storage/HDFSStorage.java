/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.storage;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import javax.validation.constraints.NotNull;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.flume.sink.Server;

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
  public static final String BASE_DIR_KEY = "baseDir";
  public static final String RESTORE_KEY = "restore";
  public static final String BLOCKSIZE = "blockSize";
  
  private static final String IDENTITY_FILE = "counter";
  private static final String CLEAN_FILE = "clean-counter";
  private static final String OFFSET_SUFFIX = "-offsetFile";
  private static final String CLEAN_OFFSET_FILE = "cleanoffsetFile";
  public static final String OFFSET_KEY = "offset";
  private static final int IDENTIFIER_SIZE = 8;
  private static final int DATA_LENGTH_BYTE_SIZE = 4;
  /**
   * Identifier for this storage.
   */
  @NotNull
  private String id;
  /**
   * The baseDir where the storage facility is going to create files.
   */
  @NotNull
  private String baseDir;
  /**
   * The block size to be used to create the storage files
   */
  private long blockSize;
  /**
   *
   */
  private boolean restore;
  /**
   * This identifies the current file number
   */
  private long currentWrittenFile;
  /**
   * This identifies the file number that has been flushed
   */
  private long flushedFileCounter;
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
  ArrayList<DataBlock> files2Commit = new ArrayList<DataBlock>();
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
  private transient Path basePath;

  public HDFSStorage()
  {
    this.blockSize = 64 * 1024 * 1024;
    this.restore = true;
  }

  /**
   * This stores the Identifier information identified in the last store function call
   *
   * @param ctx
   */
  // private byte[] fileOffset = new byte[IDENTIFIER_SIZE];
  @Override
  public void configure(Context ctx)
  {
    String tempId = ctx.getString(ID);
    if (tempId == null) {
      if (id == null) {
        throw new IllegalArgumentException("id can't be  null.");
      }
    }
    else {
      id = tempId;
    }

    Configuration conf = new Configuration();

    String tempBaseDir = ctx.getString(BASE_DIR_KEY);
    if (tempBaseDir == null) {
      if (baseDir == null) {
        baseDir = conf.get("hadoop.tmp.dir");
        if (baseDir == null || baseDir.isEmpty()) {
          throw new IllegalArgumentException("baseDir cannot be null.");
        }
      }
    }
    else {
      baseDir = tempBaseDir;
    }

    // offset = ctx.getInteger(OFFSET_KEY, 4);
    offset = 4;
    skipOffset = -1;
    skipFile = -1;

    try {
      fs = FileSystem.get(conf);
      Path path = new Path(baseDir);
      if (!fs.exists(path)) {
        throw new RuntimeException(String.format("baseDir passed (%s) doesn't exist.", baseDir));
      }
      if (!fs.isDirectory(path)) {
        throw new RuntimeException(String.format("baseDir passed (%s) is not a directory.", baseDir));
      }

      basePath = new Path(path, id);

      restore = ctx.getBoolean(RESTORE_KEY, restore);
      if (!restore) {
        fs.delete(basePath, true);
      }
      if (!fs.exists(basePath) || !fs.isDirectory(basePath)) {
        fs.mkdirs(basePath);
      }

      long tempBlockSize = ctx.getLong(BLOCKSIZE, fs.getDefaultBlockSize(basePath));
      if (tempBlockSize > 0) {
        blockSize = tempBlockSize;
      }

      currentWrittenFile = 0;
      cleanedFileCounter = -1;
      retrievalFile = -1;
      fileCounterFile = new Path(basePath, IDENTITY_FILE);
      cleanFileCounterFile = new Path(basePath, CLEAN_FILE);
      cleanFileOffsetFile = new Path(basePath, CLEAN_OFFSET_FILE);
      if (restore) {
        if (fs.exists(fileCounterFile) && fs.isFile(fileCounterFile)) {
          currentWrittenFile = Long.valueOf(new String(readData(fileCounterFile)));
        }

        if (fs.exists(cleanFileCounterFile) && fs.isFile(cleanFileCounterFile)) {
          cleanedFileCounter = Long.valueOf(new String(readData(cleanFileCounterFile)));
        }

        if (fs.exists(cleanFileOffsetFile) && fs.isFile(cleanFileOffsetFile)) {
          cleanedOffset = readData(cleanFileOffsetFile);
        }
      }
      flushedFileCounter = currentWrittenFile;
    }
    catch (IOException io) {
      throw new RuntimeException(io);
    }
  }

  /**
   * This function reads the file at a location and return the bytes stored in the file "
   *
   * @param path
   * - the location of the file
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
   * the file location
   * @param data
   * the data to be written to the file
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
    int bytesToWrite = bytes.length + DATA_LENGTH_BYTE_SIZE;
    if (fileWriteOffset + bytesToWrite < blockSize) {
      try {
        /* write length and the actual data to the file */
        if (fileWriteOffset == 0) {
          dataStream = writeData(new Path(basePath, String.valueOf(currentWrittenFile)), Ints.toByteArray(bytes.length));
          dataStream.write(bytes);
        }
        else {
          dataStream.write(Ints.toByteArray(bytes.length));
          dataStream.write(bytes);
        }
        fileWriteOffset += bytesToWrite;

        byte[] fileOffset = null;
        if (currentWrittenFile > skipFile || (currentWrittenFile == skipFile && fileWriteOffset > skipOffset)) {
          fileOffset = new byte[IDENTIFIER_SIZE];
          Server.writeLong(fileOffset, 0, calculateOffset(fileWriteOffset, currentWrittenFile));
        }
        return fileOffset;
      }
      catch (IOException ex) {
        logger.warn("Error while storing the bytes {}", ex.getMessage());
        throw new RuntimeException(ex);
      }
    }
    files2Commit.add(new DataBlock(dataStream, fileWriteOffset, new Path(basePath, currentWrittenFile + OFFSET_SUFFIX), currentWrittenFile));
    fileWriteOffset = 0;
    ++currentWrittenFile;
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

    if (retrievalFile == 0 && retrievalOffset == 0 && currentWrittenFile == 0 && fileWriteOffset == 0) {
      skipOffset = 0;
      return null;
    }

    // flushing the last incomplete flushed file
    closeUnflushedFiles();

    if ((retrievalFile > currentWrittenFile) || (retrievalFile == currentWrittenFile && retrievalOffset >= fileWriteOffset)) {
      skipFile = retrievalFile;
      skipOffset = retrievalOffset;
      return null;
    }


    // socho... jor lagake.
//    if (retrievalFile >= flushedFileCounter && retrievalFile <= currentWrittenFile) {
//      logger.warn("data not flushed for the given identifier");
//      return null;
//    }
//
    // making sure that the deleted address is not requested again
    if (retrievalFile != 0 || retrievalOffset != 0) {
      long cleanedFile = byteArrayToLong(cleanedOffset, offset);
      if (retrievalFile < cleanedFile || (retrievalFile == cleanedFile && retrievalOffset < byteArrayToLong(cleanedOffset, 0))) {
        logger.warn("The address asked has been deleted");
        throw new IllegalArgumentException(String.format("The data for address %s has already been deleted", Arrays.toString(identifier)));
      }
    }

    // we have just started
    if (retrievalFile == 0 && retrievalOffset == 0) {
      retrievalFile = byteArrayToLong(cleanedOffset, offset);
      retrievalOffset = byteArrayToLong(cleanedOffset, 0);
    }

    try {
      if (readStream != null) {
        readStream.close();
      }
      Path path = new Path(basePath, String.valueOf(retrievalFile));
      if (!fs.exists(path)) {
        retrievalFile = -1;
        throw new RuntimeException(String.format("File %s does not exist", path.toString()));
      }

      byte[] flushedOffset = readData(new Path(basePath, retrievalFile + OFFSET_SUFFIX));
      flushedLong = Server.readLong(flushedOffset, 0);
      while (retrievalOffset >= flushedLong && retrievalFile < flushedFileCounter) {
        retrievalFile++;
        retrievalOffset -= flushedLong;
        flushedOffset = readData(new Path(basePath, retrievalFile + OFFSET_SUFFIX));
        flushedLong = Server.readLong(flushedOffset, 0);
      }

      if (retrievalFile >= flushedFileCounter) {
        logger.warn("data not flushed for the given identifier");
        return null;
      }
      readStream = new FSDataInputStream(fs.open(path));
      readStream.seek(retrievalOffset);
      return retrieveHelper();
    }
    catch (IOException e) {
      logger.warn(e.getMessage());
      try {
        readStream.close();
      }
      catch (IOException io) {
        logger.warn("Failed Close", io);
      }
      finally {
        retrievalFile = -1;
        readStream = null;
      }
      return null;
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
      Server.writeLong(data, 0, calculateOffset(0, retrievalFile + 1));
    }
    else {
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

    if (retrievalFile >= flushedFileCounter) {
      logger.warn("data is not flushed");
      return null;
    }

    try {
      if (readStream == null) {
        readStream = new FSDataInputStream(fs.open(new Path(basePath, String.valueOf(retrievalFile))));
        byte[] flushedOffset = readData(new Path(basePath, retrievalFile + OFFSET_SUFFIX));
        flushedLong = Server.readLong(flushedOffset, 0);
      }

      if (retrievalOffset >= flushedLong) {
        retrievalFile++;
        retrievalOffset = 0;

        if (retrievalFile >= flushedFileCounter) {
          logger.warn("data is not flushed");
          return null;
        }

        readStream.close();
        readStream = new FSDataInputStream(fs.open(new Path(basePath, String.valueOf(retrievalFile))));
        byte[] flushedOffset = readData(new Path(basePath, retrievalFile + OFFSET_SUFFIX));
        flushedLong = Server.readLong(flushedOffset, 0);
      }
      readStream.seek(retrievalOffset);
      return retrieveHelper();
    }
    catch (IOException e) {
      logger.warn(" error while retrieving {}", e.getMessage());
      return null;
    }
  }

  @Override
  @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
  public void clean(byte[] identifier)
  {
    long cleanFileIndex = byteArrayToLong(identifier, offset);
    if (cleanedFileCounter >= cleanFileIndex) {
      return;
    }
    try {
      do {
        Path path = new Path(basePath, String.valueOf(cleanedFileCounter));
        if (fs.exists(path) && fs.isFile(path)) {
          fs.delete(path, false);
        }
        path = new Path(basePath, cleanedFileCounter + OFFSET_SUFFIX);
        if (fs.exists(path) && fs.isFile(path)) {
          fs.delete(path, false);
        }
        ++cleanedFileCounter;
      }
      while (cleanedFileCounter < cleanFileIndex);
      writeData(cleanFileCounterFile, String.valueOf(cleanedFileCounter).getBytes()).close();
      writeData(cleanFileOffsetFile, identifier).close();
    }
    catch (IOException e) {
      logger.warn("not able to close the streams {}", e.getMessage());
      throw new RuntimeException(e);
    }
    finally {
      cleanedOffset = identifier;
    }
  }

  /**
   * This is used mainly for cleaning up of counter files created
   */
  void cleanHelperFiles()
  {
    try {
      fs.delete(basePath, true);
    }
    catch (IOException e) {
      logger.warn(e.getMessage());
    }
  }

  private void closeUnflushedFiles()
  {
    try {
      dataStream.close();

      for (DataBlock openStream : files2Commit) {
        openStream.dataStream.close();
      }
      files2Commit.clear();

      writeData(fileCounterFile, String.valueOf(flushedFileCounter + 1).getBytes()).close();
      ++flushedFileCounter;
      currentWrittenFile = flushedFileCounter;
      fileWriteOffset = 0;
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void flush()
  {
    if (dataStream != null) {
      try {
        dataStream.hflush();
        writeData(fileCounterFile, String.valueOf(currentWrittenFile + 1).getBytes()).close();
        updateFlushedOffset(new Path(basePath, currentWrittenFile + OFFSET_SUFFIX), fileWriteOffset);
      }
      catch (IOException ex) {
        logger.warn("not able to close the stream {}", ex.getMessage());
        throw new RuntimeException(ex);
      }
    }

    Iterator<DataBlock> itr = files2Commit.iterator();
    while (itr.hasNext()) {
      itr.next().close();
    }
    flushedFileCounter = currentWrittenFile;
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
    }
    catch (IOException e) {
      try {
        if (Long.valueOf(new String(readData(file))) == Long.valueOf(new String(lastStoredOffset))) {
        }
      }
      catch (NumberFormatException e1) {
        throw new RuntimeException(e1);
      }
      catch (IOException e1) {
        throw new RuntimeException(e1);
      }
    }
  }

  /**
   * @return the baseDir
   */
  public String getBaseDir()
  {
    return baseDir;
  }

  /**
   * @param baseDir the baseDir to set
   */
  public void setBaseDir(String baseDir)
  {
    this.baseDir = baseDir;
  }

  /**
   * @return the id
   */
  public String getId()
  {
    return id;
  }

  /**
   * @param id the id to set
   */
  public void setId(String id)
  {
    this.id = id;
  }

  /**
   * @return the blockSize
   */
  public long getBlockSize()
  {
    return blockSize;
  }

  /**
   * @param blockSize the blockSize to set
   */
  public void setBlockSize(long blockSize)
  {
    this.blockSize = blockSize;
  }

  /**
   * @return the restore
   */
  public boolean isRestore()
  {
    return restore;
  }

  /**
   * @param restore the restore to set
   */
  public void setRestore(boolean restore)
  {
    this.restore = restore;
  }

  class DataBlock
  {
    FSDataOutputStream dataStream;
    long dataOffset;
    Path flushedData;
    long fileName;

    DataBlock(FSDataOutputStream stream, long bytesWritten, Path path2FlushedData, long fileName)
    {
      this.dataStream = stream;
      this.dataOffset = bytesWritten;
      this.flushedData = path2FlushedData;
      this.fileName = fileName;
    }

    public void close()
    {
      if (dataStream != null) {
        try {
          dataStream.close();
          updateFlushedOffset(flushedData, dataOffset);

        }
        catch (IOException ex) {
          logger.warn("not able to close the stream {}", ex.getMessage());
          throw new RuntimeException(ex);
        }
      }
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(HDFSStorage.class);
}
