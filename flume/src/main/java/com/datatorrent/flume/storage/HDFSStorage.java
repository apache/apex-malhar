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

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

public class HDFSStorage implements Storage, Configurable
{
  private static final String identityFileName = "/counter";
  private static final String cleanFileName = "/clean-counter";
  public static final String BASE_DIR_KEY = "baseDir";
  public static final String OFFSET_KEY = "offset";
  public static final String RESTORE_KEY = "restore";
  public static final String BLOCKSIZE = "blockSize";
  private String baseDir;
  private long fileCounter;
  private Path fileCounterFile;
  private Path cleanFileCounterFile;
  private FileSystem fs;
  private FSDataOutputStream dataStream;
  private long blockSize;
  private long fileWriteOffset;
  private FSDataInputStream readStream;
  private long retrievalOffset;
  private long retrievalFile;
  private long cleanedFileCounter;
  private int offset;

  @Override
  public void configure(Context ctx)
  {
    Configuration conf = new Configuration();
    baseDir = ctx.getString(BASE_DIR_KEY, conf.get("hadoop.tmp.dir"));
    // offset = ctx.getInteger(OFFSET_KEY, 4);
    offset = 4;

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
        if (restore) {
          if (fs.exists(fileCounterFile) && fs.isFile(fileCounterFile)) {
            fileCounter = Long.valueOf(new String(readData(fileCounterFile)));
          }
          if (fs.exists(cleanFileCounterFile) && fs.isFile(cleanFileCounterFile)) {
            cleanedFileCounter = Long.valueOf(new String(readData(cleanFileCounterFile)));
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

  private byte[] longToByteArray(long fileOffset, long fileCounter)
  {
    long l = (fileCounter << 32) | (fileOffset & 0xffffffffl);
    return Longs.toByteArray(l);
  }

  @Override
  public byte[] store(byte[] bytes)
  {
    // 4 for the number of bytes used to store the length of the data
    if (fileWriteOffset + bytes.length + 4 < blockSize) {
      try {

        if (fileWriteOffset == 0) {
          dataStream = writeData(new Path(baseDir + "/" + String.valueOf(fileCounter)), Ints.toByteArray(bytes.length));
          dataStream.write(bytes);
          writeData(fileCounterFile, String.valueOf(fileCounter + 1).getBytes()).close();
        } else {
          dataStream.write(Ints.toByteArray(bytes.length));
          dataStream.write(bytes);
        }
        byte[] fileOffset = longToByteArray(fileWriteOffset, fileCounter);
        fileWriteOffset += (bytes.length + 4);
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
    byte b1 = 0;
    return Longs.fromBytes(b1, b1, b1, b1, b[0 + startIndex], b[1+startIndex], b[2+startIndex], b[3+startIndex]);
     
  }

  @Override
  public byte[] retrieve(byte[] identifier)
  {
    retrievalOffset = byteArrayToLong(identifier, offset);
    retrievalFile = byteArrayToLong(identifier, 0);
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
      throw new RuntimeException(e);
    }
  }

  private byte[] retrieveHelper() throws IOException
  {
    int length = readStream.readInt();
    byte[] data = new byte[length + 8];
    int readSize = readStream.read(data, 8, length);
    if (readSize == -1) {
      throw new RuntimeException("Invalid identifier");
    }
    retrievalOffset += length + 4;
    byte[] fileOffset = longToByteArray(retrievalOffset, retrievalFile);
    System.arraycopy(fileOffset, 0, data, 0, 8);
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
        retrievalOffset = 0;
        retrievalFile++;
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
    long cleanFileIndex = byteArrayToLong(identifier, 0);
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
