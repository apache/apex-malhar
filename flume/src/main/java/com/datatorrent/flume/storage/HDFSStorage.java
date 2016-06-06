/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.storage;

import java.io.DataInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import com.datatorrent.api.Component;
import com.datatorrent.common.util.NameableThreadFactory;
import com.datatorrent.flume.sink.Server;
import com.datatorrent.netlet.util.Slice;

/**
 * HDFSStorage is developed to store and retrieve the data from HDFS
 * <p />
 * The properties that can be set on HDFSStorage are: <br />
 * baseDir - The base directory where the data is going to be stored <br />
 * restore - This is used to restore the application from previous failure <br />
 * blockSize - The maximum size of the each file to created. <br />
 *
 * @author Gaurav Gupta <gaurav@datatorrent.com>
 * @since 0.9.3
 */
public class HDFSStorage implements Storage, Configurable, Component<com.datatorrent.api.Context>
{
  public static final int DEFAULT_BLOCK_SIZE = 64 * 1024 * 1024;
  public static final String BASE_DIR_KEY = "baseDir";
  public static final String RESTORE_KEY = "restore";
  public static final String BLOCKSIZE = "blockSize";
  public static final String BLOCK_SIZE_MULTIPLE = "blockSizeMultiple";
  public static final String NUMBER_RETRY = "retryCount";

  private static final String OFFSET_SUFFIX = "-offsetFile";
  private static final String BOOK_KEEPING_FILE_OFFSET = "-bookKeepingOffsetFile";
  private static final String FLUSHED_IDENTITY_FILE = "flushedCounter";
  private static final String CLEAN_OFFSET_FILE = "cleanoffsetFile";
  private static final String FLUSHED_IDENTITY_FILE_TEMP = "flushedCounter.tmp";
  private static final String CLEAN_OFFSET_FILE_TEMP = "cleanoffsetFile.tmp";
  private static final int IDENTIFIER_SIZE = 8;
  private static final int DATA_LENGTH_BYTE_SIZE = 4;

  /**
   * Number of times the storage will try to get the filesystem
   */
  private int retryCount = 3;
  /**
   * The multiple of block size
   */
  private int blockSizeMultiple = 1;
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
  // private Path fileCounterFile;
  /**
   * The file that stores the flushed fileCounter information
   */
  private Path flushedCounterFile;
  private Path flushedCounterFileTemp;
  /**
   * This identifies the last cleaned file number
   */
  private long cleanedFileCounter;
  /**
   * The file that stores the clean file counter information
   */
  // private Path cleanFileCounterFile;
  /**
   * The file that stores the clean file offset information
   */
  private Path cleanFileOffsetFile;
  private Path cleanFileOffsetFileTemp;
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
  private long flushedFileWriteOffset;
  private long bookKeepingFileOffset;
  private byte[] cleanedOffset = new byte[8];
  private long skipOffset;
  private long skipFile;
  private transient Path basePath;
  private ExecutorService storageExecutor;
  private byte[] currentData;
  private FSDataInputStream nextReadStream;
  private long nextFlushedLong;
  private long nextRetrievalFile;
  private byte[] nextRetrievalData;

  public HDFSStorage()
  {
    this.restore = true;
  }

  /**
   * This stores the Identifier information identified in the last store function call
   *
   * @param ctx
   */
  @Override
  public void configure(Context ctx)
  {
    String tempId = ctx.getString(ID);
    if (tempId == null) {
      if (id == null) {
        throw new IllegalArgumentException("id can't be  null.");
      }
    } else {
      id = tempId;
    }

    String tempBaseDir = ctx.getString(BASE_DIR_KEY);
    if (tempBaseDir != null) {
      baseDir = tempBaseDir;
    }

    restore = ctx.getBoolean(RESTORE_KEY, restore);
    Long tempBlockSize = ctx.getLong(BLOCKSIZE);
    if (tempBlockSize != null) {
      blockSize = tempBlockSize;
    }
    blockSizeMultiple = ctx.getInteger(BLOCK_SIZE_MULTIPLE, blockSizeMultiple);
    retryCount = ctx.getInteger(NUMBER_RETRY,retryCount);
  }

  /**
   * This function reads the file at a location and return the bytes stored in the file "
   *
   * @param path - the location of the file
   * @return
   * @throws IOException
   */
  byte[] readData(Path path) throws IOException
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
   * @param path the file location
   * @param data the data to be written to the file
   * @return
   * @throws IOException
   */
  private FSDataOutputStream writeData(Path path, byte[] data) throws IOException
  {
    FSDataOutputStream fsOutputStream;
    if (fs.getScheme().equals("file")) {
      // local FS does not support hflush and does not flush native stream
      fsOutputStream = new FSDataOutputStream(
          new FileOutputStream(Path.getPathWithoutSchemeAndAuthority(path).toString()), null);
    } else {
      fsOutputStream = fs.create(path);
    }
    fsOutputStream.write(data);
    return fsOutputStream;
  }

  private long calculateOffset(long fileOffset, long fileCounter)
  {
    return ((fileCounter << 32) | (fileOffset & 0xffffffffL));
  }

  @Override
  public byte[] store(Slice slice)
  {
    // logger.debug("store message ");
    int bytesToWrite = slice.length + DATA_LENGTH_BYTE_SIZE;
    if (currentWrittenFile < skipFile) {
      fileWriteOffset += bytesToWrite;
      if (fileWriteOffset >= bookKeepingFileOffset) {
        files2Commit.add(new DataBlock(null, bookKeepingFileOffset,
            new Path(basePath, currentWrittenFile + OFFSET_SUFFIX), currentWrittenFile));
        currentWrittenFile++;
        if (fileWriteOffset > bookKeepingFileOffset) {
          fileWriteOffset = bytesToWrite;
        } else {
          fileWriteOffset = 0;
        }
        try {
          bookKeepingFileOffset = getFlushedFileWriteOffset(
              new Path(basePath, currentWrittenFile + BOOK_KEEPING_FILE_OFFSET));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      return null;
    }

    if (flushedFileCounter == currentWrittenFile && dataStream == null) {
      currentWrittenFile++;
      fileWriteOffset = 0;
    }

    if (flushedFileCounter == skipFile && skipFile != -1) {
      skipFile++;
    }

    if (fileWriteOffset + bytesToWrite < blockSize) {
      try {
        /* write length and the actual data to the file */
        if (fileWriteOffset == 0) {
          // writeData(flushedCounterFile, String.valueOf(currentWrittenFile).getBytes()).close();
          dataStream = writeData(new Path(basePath, String.valueOf(currentWrittenFile)),
              Ints.toByteArray(slice.length));
          dataStream.write(slice.buffer, slice.offset, slice.length);
        } else {
          dataStream.write(Ints.toByteArray(slice.length));
          dataStream.write(slice.buffer, slice.offset, slice.length);
        }
        fileWriteOffset += bytesToWrite;

        byte[] fileOffset = null;
        if ((currentWrittenFile > skipFile) || (currentWrittenFile == skipFile && fileWriteOffset > skipOffset)) {
          skipFile = -1;
          fileOffset = new byte[IDENTIFIER_SIZE];
          Server.writeLong(fileOffset, 0, calculateOffset(fileWriteOffset, currentWrittenFile));
        }
        return fileOffset;
      } catch (IOException ex) {
        logger.warn("Error while storing the bytes {}", ex.getMessage());
        closeFs();
        throw new RuntimeException(ex);
      }
    }
    DataBlock db = new DataBlock(dataStream, fileWriteOffset,
        new Path(basePath, currentWrittenFile + OFFSET_SUFFIX), currentWrittenFile);
    db.close();
    files2Commit.add(db);
    fileWriteOffset = 0;
    ++currentWrittenFile;
    return store(slice);
  }

  /**
   * @param b
   * @param startIndex
   * @return
   */
  long byteArrayToLong(byte[] b, int startIndex)
  {
    final byte b1 = 0;
    return Longs.fromBytes(b1, b1, b1, b1, b[3 + startIndex], b[2 + startIndex], b[1 + startIndex], b[startIndex]);
  }

  @Override
  public byte[] retrieve(byte[] identifier)
  {
    skipFile = -1;
    skipOffset = 0;
    logger.debug("retrieve with address {}", Arrays.toString(identifier));
    // flushing the last incomplete flushed file
    closeUnflushedFiles();

    retrievalOffset = byteArrayToLong(identifier, 0);
    retrievalFile = byteArrayToLong(identifier, offset);

    if (retrievalFile == 0 && retrievalOffset == 0 && currentWrittenFile == 0 && fileWriteOffset == 0) {
      skipOffset = 0;
      return null;
    }

    // making sure that the deleted address is not requested again
    if (retrievalFile != 0 || retrievalOffset != 0) {
      long cleanedFile = byteArrayToLong(cleanedOffset, offset);
      if (retrievalFile < cleanedFile || (retrievalFile == cleanedFile &&
          retrievalOffset < byteArrayToLong(cleanedOffset, 0))) {
        logger.warn("The address asked has been deleted retrievalFile={}, cleanedFile={}, retrievalOffset={}, " +
            "cleanedOffset={}", retrievalFile, cleanedFile, retrievalOffset, byteArrayToLong(cleanedOffset, 0));
        closeFs();
        throw new IllegalArgumentException(String.format("The data for address %s has already been deleted",
            Arrays.toString(identifier)));
      }
    }

    // we have just started
    if (retrievalFile == 0 && retrievalOffset == 0) {
      retrievalFile = byteArrayToLong(cleanedOffset, offset);
      retrievalOffset = byteArrayToLong(cleanedOffset, 0);
    }

    if ((retrievalFile > flushedFileCounter)) {
      skipFile = retrievalFile;
      skipOffset = retrievalOffset;
      retrievalFile = -1;
      return null;
    }
    if ((retrievalFile == flushedFileCounter && retrievalOffset >= flushedFileWriteOffset)) {
      skipFile = retrievalFile;
      skipOffset = retrievalOffset - flushedFileWriteOffset;
      retrievalFile = -1;
      return null;
    }

    try {
      if (readStream != null) {
        readStream.close();
        readStream = null;
      }
      Path path = new Path(basePath, String.valueOf(retrievalFile));
      if (!fs.exists(path)) {
        retrievalFile = -1;
        closeFs();
        throw new RuntimeException(String.format("File %s does not exist", path.toString()));
      }

      byte[] flushedOffset = readData(new Path(basePath, retrievalFile + OFFSET_SUFFIX));
      flushedLong = Server.readLong(flushedOffset, 0);
      while (retrievalOffset >= flushedLong && retrievalFile < flushedFileCounter) {
        retrievalOffset -= flushedLong;
        retrievalFile++;
        flushedOffset = readData(new Path(basePath, retrievalFile + OFFSET_SUFFIX));
        flushedLong = Server.readLong(flushedOffset, 0);
      }

      if (retrievalOffset >= flushedLong) {
        logger.warn("data not flushed for the given identifier");
        retrievalFile = -1;
        return null;
      }
      synchronized (HDFSStorage.this) {
        if (nextReadStream != null) {
          nextReadStream.close();
          nextReadStream = null;
        }
      }
      currentData = null;
      path = new Path(basePath, String.valueOf(retrievalFile));
      //readStream = new FSDataInputStream(fs.open(path));
      currentData = readData(path);
      //readStream.seek(retrievalOffset);
      storageExecutor.submit(getNextStream());
      return retrieveHelper();
    } catch (IOException e) {
      closeFs();
      throw new RuntimeException(e);
    }
  }

  private byte[] retrieveHelper() throws IOException
  {
    int tempRetrievalOffset = (int)retrievalOffset;
    int length = Ints.fromBytes(currentData[tempRetrievalOffset], currentData[tempRetrievalOffset + 1],
        currentData[tempRetrievalOffset + 2], currentData[tempRetrievalOffset + 3]);
    byte[] data = new byte[length + IDENTIFIER_SIZE];
    System.arraycopy(currentData, tempRetrievalOffset + 4, data, IDENTIFIER_SIZE, length);
    retrievalOffset += length + DATA_LENGTH_BYTE_SIZE;
    if (retrievalOffset >= flushedLong) {
      Server.writeLong(data, 0, calculateOffset(0, retrievalFile + 1));
    } else {
      Server.writeLong(data, 0, calculateOffset(retrievalOffset, retrievalFile));
    }
    return data;
  }

  @Override
  public byte[] retrieveNext()
  {
    if (retrievalFile == -1) {
      closeFs();
      throw new RuntimeException("Call retrieve first");
    }

    if (retrievalFile > flushedFileCounter) {
      logger.warn("data is not flushed");
      return null;
    }

    try {
      if (currentData == null) {
        synchronized (HDFSStorage.this) {
          if (nextRetrievalData != null && (retrievalFile == nextRetrievalFile)) {
            currentData = nextRetrievalData;
            flushedLong = nextFlushedLong;
            nextRetrievalData = null;
          } else {
            currentData = null;
            currentData = readData(new Path(basePath, String.valueOf(retrievalFile)));
            byte[] flushedOffset = readData(new Path(basePath, retrievalFile + OFFSET_SUFFIX));
            flushedLong = Server.readLong(flushedOffset, 0);
          }
        }
        storageExecutor.submit(getNextStream());
      }

      if (retrievalOffset >= flushedLong) {
        retrievalFile++;
        retrievalOffset = 0;

        if (retrievalFile > flushedFileCounter) {
          logger.warn("data is not flushed");
          return null;
        }

        //readStream.close();
        // readStream = new FSDataInputStream(fs.open(new Path(basePath, String.valueOf(retrievalFile))));
        // byte[] flushedOffset = readData(new Path(basePath, retrievalFile + OFFSET_SUFFIX));
        // flushedLong = Server.readLong(flushedOffset, 0);

        synchronized (HDFSStorage.this) {
          if (nextRetrievalData != null && (retrievalFile == nextRetrievalFile)) {
            currentData = nextRetrievalData;
            flushedLong = nextFlushedLong;
            nextRetrievalData = null;
          } else {
            currentData = null;
            currentData = readData(new Path(basePath, String.valueOf(retrievalFile)));
            byte[] flushedOffset = readData(new Path(basePath, retrievalFile + OFFSET_SUFFIX));
            flushedLong = Server.readLong(flushedOffset, 0);
          }
        }
        storageExecutor.submit(getNextStream());
      }
      //readStream.seek(retrievalOffset);
      return retrieveHelper();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
  public void clean(byte[] identifier)
  {
    logger.info("clean {}", Arrays.toString(identifier));
    long cleanFileIndex = byteArrayToLong(identifier, offset);

    long cleanFileOffset = byteArrayToLong(identifier, 0);
    if (flushedFileCounter == -1) {
      identifier = new byte[8];
    } else if (cleanFileIndex > flushedFileCounter ||
        (cleanFileIndex == flushedFileCounter && cleanFileOffset >= flushedFileWriteOffset)) {
      // This is to make sure that we clean only the data that is flushed
      cleanFileIndex = flushedFileCounter;
      cleanFileOffset = flushedFileWriteOffset;
      Server.writeLong(identifier, 0, calculateOffset(cleanFileOffset, cleanFileIndex));
    }
    cleanedOffset = identifier;

    try {
      writeData(cleanFileOffsetFileTemp, identifier).close();
      fs.rename(cleanFileOffsetFileTemp, cleanFileOffsetFile);
      if (cleanedFileCounter >= cleanFileIndex) {
        return;
      }
      do {
        Path path = new Path(basePath, String.valueOf(cleanedFileCounter));
        if (fs.exists(path) && fs.isFile(path)) {
          fs.delete(path, false);
        }
        path = new Path(basePath, cleanedFileCounter + OFFSET_SUFFIX);
        if (fs.exists(path) && fs.isFile(path)) {
          fs.delete(path, false);
        }
        path = new Path(basePath, cleanedFileCounter + BOOK_KEEPING_FILE_OFFSET);
        if (fs.exists(path) && fs.isFile(path)) {
          fs.delete(path, false);
        }
        logger.info("deleted file {}", cleanedFileCounter);
        ++cleanedFileCounter;
      } while (cleanedFileCounter < cleanFileIndex);
      // writeData(cleanFileCounterFile, String.valueOf(cleanedFileCounter).getBytes()).close();

    } catch (IOException e) {
      logger.warn("not able to close the streams {}", e.getMessage());
      closeFs();
      throw new RuntimeException(e);
    }
  }

  /**
   * This is used mainly for cleaning up of counter files created
   */
  void cleanHelperFiles()
  {
    try {
      fs.delete(basePath, true);
    } catch (IOException e) {
      logger.warn(e.getMessage());
    }
  }

  private void closeUnflushedFiles()
  {
    try {
      files2Commit.clear();
      // closing the stream
      if (dataStream != null) {
        dataStream.close();
        dataStream = null;
        // currentWrittenFile++;
        // fileWriteOffset = 0;
      }

      if (!fs.exists(new Path(basePath, currentWrittenFile + OFFSET_SUFFIX))) {
        fs.delete(new Path(basePath, String.valueOf(currentWrittenFile)), false);
      }

      if (fs.exists(new Path(basePath, flushedFileCounter + OFFSET_SUFFIX))) {
        // This means that flush was called
        flushedFileWriteOffset = getFlushedFileWriteOffset(new Path(basePath, flushedFileCounter + OFFSET_SUFFIX));
        bookKeepingFileOffset = getFlushedFileWriteOffset(
            new Path(basePath, flushedFileCounter + BOOK_KEEPING_FILE_OFFSET));
      }

      if (flushedFileCounter != -1) {
        currentWrittenFile = flushedFileCounter;
        fileWriteOffset = flushedFileWriteOffset;
      } else {
        currentWrittenFile = 0;
        fileWriteOffset = 0;
      }

      flushedLong = 0;

    } catch (IOException e) {
      closeFs();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void flush()
  {
    nextReadStream = null;
    StringBuilder builder = new StringBuilder();
    Iterator<DataBlock> itr = files2Commit.iterator();
    DataBlock db;
    try {
      while (itr.hasNext()) {
        db = itr.next();
        db.updateOffsets();
        builder.append(db.fileName).append(", ");
      }
      files2Commit.clear();

      if (dataStream != null) {
        dataStream.hflush();
        writeData(flushedCounterFileTemp, String.valueOf(currentWrittenFile).getBytes()).close();
        fs.rename(flushedCounterFileTemp, flushedCounterFile);
        updateFlushedOffset(new Path(basePath, currentWrittenFile + OFFSET_SUFFIX), fileWriteOffset);
        flushedFileWriteOffset = fileWriteOffset;
        builder.append(currentWrittenFile);
      }
      logger.debug("flushed files {}", builder.toString());
    } catch (IOException ex) {
      logger.warn("not able to close the stream {}", ex.getMessage());
      closeFs();
      throw new RuntimeException(ex);
    }
    flushedFileCounter = currentWrittenFile;
    // logger.debug("flushedFileCounter in flush {}",flushedFileCounter);
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
        if (!Arrays.equals(readData(file), lastStoredOffset)) {
          closeFs();
          throw new RuntimeException(e);
        }
      } catch (Exception e1) {
        closeFs();
        throw new RuntimeException(e1);
      }
    }
  }

  public int getBlockSizeMultiple()
  {
    return blockSizeMultiple;
  }

  public void setBlockSizeMultiple(int blockSizeMultiple)
  {
    this.blockSizeMultiple = blockSizeMultiple;
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
    Path path2FlushedData;
    long fileName;
    private Path bookKeepingPath;

    DataBlock(FSDataOutputStream stream, long bytesWritten, Path path2FlushedData, long fileName)
    {
      this.dataStream = stream;
      this.dataOffset = bytesWritten;
      this.path2FlushedData = path2FlushedData;
      this.fileName = fileName;
    }

    public void close()
    {
      if (dataStream != null) {
        try {
          dataStream.close();
          bookKeepingPath = new Path(basePath, fileName + BOOK_KEEPING_FILE_OFFSET);
          updateFlushedOffset(bookKeepingPath, dataOffset);
        } catch (IOException ex) {
          logger.warn("not able to close the stream {}", ex.getMessage());
          closeFs();
          throw new RuntimeException(ex);
        }
      }
    }

    public void updateOffsets() throws IOException
    {
      updateFlushedOffset(path2FlushedData, dataOffset);
      if (bookKeepingPath != null && fs.exists(bookKeepingPath)) {
        fs.delete(bookKeepingPath, false);
      }
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(HDFSStorage.class);

  @Override
  public void setup(com.datatorrent.api.Context context)
  {
    Configuration conf = new Configuration();
    if (baseDir == null) {
      baseDir = conf.get("hadoop.tmp.dir");
      if (baseDir == null || baseDir.isEmpty()) {
        throw new IllegalArgumentException("baseDir cannot be null.");
      }
    }
    offset = 4;
    skipOffset = -1;
    skipFile = -1;
    int tempRetryCount = 0;
    while (tempRetryCount < retryCount && fs == null) {
      try {
        fs = FileSystem.newInstance(conf);
        tempRetryCount++;
      } catch (Throwable throwable) {
        logger.warn("Not able to get file system ", throwable);
      }
    }

    try {
      Path path = new Path(baseDir);
      basePath = new Path(path, id);
      if (fs == null) {
        fs = FileSystem.newInstance(conf);
      }
      if (!fs.exists(path)) {
        closeFs();
        throw new RuntimeException(String.format("baseDir passed (%s) doesn't exist.", baseDir));
      }
      if (!fs.isDirectory(path)) {
        closeFs();
        throw new RuntimeException(String.format("baseDir passed (%s) is not a directory.", baseDir));
      }
      if (!restore) {
        fs.delete(basePath, true);
      }
      if (!fs.exists(basePath) || !fs.isDirectory(basePath)) {
        fs.mkdirs(basePath);
      }

      if (blockSize == 0) {
        blockSize = fs.getDefaultBlockSize(new Path(basePath, "tempData"));
      }
      if (blockSize == 0) {
        blockSize = DEFAULT_BLOCK_SIZE;
      }

      blockSize = blockSizeMultiple * blockSize;

      currentWrittenFile = 0;
      cleanedFileCounter = -1;
      retrievalFile = -1;
      // fileCounterFile = new Path(basePath, IDENTITY_FILE);
      flushedFileCounter = -1;
      // cleanFileCounterFile = new Path(basePath, CLEAN_FILE);
      cleanFileOffsetFile = new Path(basePath, CLEAN_OFFSET_FILE);
      cleanFileOffsetFileTemp = new Path(basePath, CLEAN_OFFSET_FILE_TEMP);
      flushedCounterFile = new Path(basePath, FLUSHED_IDENTITY_FILE);
      flushedCounterFileTemp = new Path(basePath, FLUSHED_IDENTITY_FILE_TEMP);

      if (restore) {
        //
        // if (fs.exists(fileCounterFile) && fs.isFile(fileCounterFile)) {
        // //currentWrittenFile = Long.valueOf(new String(readData(fileCounterFile)));
        // }

        if (fs.exists(cleanFileOffsetFile) && fs.isFile(cleanFileOffsetFile)) {
          cleanedOffset = readData(cleanFileOffsetFile);
        }

        if (fs.exists(flushedCounterFile) && fs.isFile(flushedCounterFile)) {
          String strFlushedFileCounter = new String(readData(flushedCounterFile));
          if (strFlushedFileCounter.isEmpty()) {
            logger.warn("empty flushed file");
          } else {
            flushedFileCounter = Long.valueOf(strFlushedFileCounter);
            flushedFileWriteOffset = getFlushedFileWriteOffset(new Path(basePath, flushedFileCounter + OFFSET_SUFFIX));
            bookKeepingFileOffset = getFlushedFileWriteOffset(
                new Path(basePath, flushedFileCounter + BOOK_KEEPING_FILE_OFFSET));
          }

        }
      }
      fileWriteOffset = flushedFileWriteOffset;
      currentWrittenFile = flushedFileCounter;
      cleanedFileCounter = byteArrayToLong(cleanedOffset, offset) - 1;
      if (currentWrittenFile == -1) {
        ++currentWrittenFile;
        fileWriteOffset = 0;
      }

    } catch (IOException io) {

      throw new RuntimeException(io);
    }
    storageExecutor = Executors.newSingleThreadExecutor(new NameableThreadFactory("StorageHelper"));
  }

  private void closeFs()
  {
    if (fs != null) {
      try {
        fs.close();
        fs = null;
      } catch (IOException e) {
        logger.debug(e.getMessage());
      }
    }
  }

  private long getFlushedFileWriteOffset(Path filePath) throws IOException
  {
    if (flushedFileCounter != -1 && fs.exists(filePath)) {
      byte[] flushedFileOffsetByte = readData(filePath);
      if (flushedFileOffsetByte != null && flushedFileOffsetByte.length == 8) {
        return Server.readLong(flushedFileOffsetByte, 0);
      }
    }
    return 0;
  }

  @Override
  public void teardown()
  {
    logger.debug("called teardown");
    try {
      if (readStream != null) {
        readStream.close();
      }
      synchronized (HDFSStorage.this) {
        if (nextReadStream != null) {
          nextReadStream.close();
        }
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      closeUnflushedFiles();
      storageExecutor.shutdown();
    }

  }

  private Runnable getNextStream()
  {
    return new Runnable()
    {
      @Override
      public void run()
      {
        try {
          synchronized (HDFSStorage.this) {
            nextRetrievalFile = retrievalFile + 1;
            if (nextRetrievalFile > flushedFileCounter) {
              nextRetrievalData = null;
              return;
            }
            Path path = new Path(basePath, String.valueOf(nextRetrievalFile));
            Path offsetPath = new Path(basePath, nextRetrievalFile + OFFSET_SUFFIX);
            nextRetrievalData = null;
            nextRetrievalData = readData(path);
            byte[] flushedOffset = readData(offsetPath);
            nextFlushedLong = Server.readLong(flushedOffset, 0);
          }
        } catch (Throwable e) {
          logger.warn("in storage executor ", e);

        }
      }
    };
  }

}

