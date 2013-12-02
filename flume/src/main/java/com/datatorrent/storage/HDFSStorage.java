package com.datatorrent.storage;

import java.io.DataInputStream;
import java.io.IOException;

import com.google.common.primitives.Ints;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSStorage implements Storage
{
  private static final String identityFileName = "/counter";

  private String baseDir;
  private long fileCounter;
  private Path fileCounterFile;
  private FileSystem fs;
  private FSDataOutputStream dataStream;
  private long blockSize;
  private long filled;
  private byte[] fileOffset = new byte[8];
  private byte[] fileData;
  private long retrievalOffset;
  private long retrievalFile;
  private long cleanedFileCounter;

  private HDFSStorage(String baseDir, boolean restore) throws IOException
  {
    if (baseDir != null && baseDir.length() > 0) {
      Configuration conf = new Configuration();
      fs = FileSystem.get(conf);
      Path path = new Path(baseDir);
      if (!fs.exists(path) || !fs.isDirectory(path)) {
        throw new IOException("baseDir passed is not directory");
      }
      /* keeping the block size 2MB less than the default block size */
      blockSize = fs.getDefaultBlockSize(path) - 2 * 1024 * 1024;
      if (blockSize <= 0) {
        throw new IOException("block size is not configured properly");
      }
      this.baseDir = baseDir;
      fileCounter = 0;
      cleanedFileCounter = -1;
      fileCounterFile = new Path(baseDir + identityFileName);
      if (restore) {
        if (fs.exists(fileCounterFile) && fs.isFile(fileCounterFile)) {
          fileCounter = Long.valueOf(new String(readData(fileCounterFile)));
        }
      }
    } else {
      throw new IOException("filepath can't be null");
    }
  }

  private byte[] readData(Path path) throws IOException
  {
    DataInputStream is = new DataInputStream(fs.open(path));
    byte[] bytes = new byte[is.available()];
    is.readFully(bytes);
    is.close();
    return bytes;
  }

  private FSDataOutputStream writeData(Path path, byte[] data) throws IOException
  {
    FSDataOutputStream stream = fs.create(path);
    stream.write(data);
    return stream;
  }

  public static Storage getInstance(String baseDir, boolean restore)
  {
    try {
      Storage storage = new HDFSStorage(baseDir, restore);
      return storage;
    } catch (IOException ex) {
      logger.error("Not able to instantiate the stroage object {}", ex.getMessage());
    }
    return null;
  }

  private void longToByteArray(long value, byte[] b, int start, int size)
  {
    for (int i = 0; i < size; i++) {
      int shift = 8 * (i);
      if (shift == 0) {
        b[i + start] = (byte) value;
      }
      else {
        b[i + start] = (byte) (value >>> shift);
      }
    }
  }

  private long byteArrayToLong(byte[] b, int size, int startIndex)
  {
    long l = 0;
    for (int i = size; i >= startIndex; i--) {
      l <<= 8;
      l ^= (long) b[i] & 0xFF;
    }
    return l;
  }

  @Override
  public long store(byte[] bytes)
  {
    // 4 for the number of bytes used to store the length of the data
    if (filled + bytes.length + 4 < blockSize) {
      try {
        longToByteArray(filled, fileOffset, 0, 4);
        if (filled == 0) {
          longToByteArray(fileCounter, fileOffset, 4, 4);
          dataStream = writeData(new Path(baseDir + "/" + String.valueOf(fileCounter)), Ints.toByteArray(bytes.length));
          dataStream.write(bytes);
          writeData(fileCounterFile, String.valueOf(fileCounter + 1).getBytes()).close();
        } else {
          dataStream.write(Ints.toByteArray(bytes.length));
          dataStream.write(bytes);
        }
        //dataStream.hflush();
        filled += (bytes.length + 4);
        return byteArrayToLong(fileOffset, 7, 0);
      } catch (IOException ex) {
        logger.warn("Error while storing the bytes {}", ex.getMessage());
        return -1;
      }
    }
    try {
      dataStream.close();
    } catch (IOException ex) {
      logger.warn("Error while closing the streams {}", ex.getMessage());
    }
    filled = 0;
    ++fileCounter;
    return store(bytes);
  }

  @Override
  public RetrievalObject retrieve(long identifier)
  {
    byte[] b = new byte[8];
    longToByteArray(identifier, b, 0, 8);
    retrievalOffset = byteArrayToLong(b, 3, 0);
    retrievalFile = byteArrayToLong(b, 7, 4);
    try {
      fileData = readData(new Path(baseDir + "/" + retrievalFile));
      byte[] lengthArr = new byte[4];
      System.arraycopy(fileData, (int) retrievalOffset, lengthArr, 0, 4);
      int length = Ints.fromByteArray(lengthArr);
      RetrievalObject obj = new RetrievalObject();
      obj.setToken(identifier);
      obj.setData(new byte[length]);
      System.arraycopy(fileData, (int) (retrievalOffset + 4), obj.getData(), 0, length);
      retrievalOffset += 4 + length;
      return obj;
    } catch (IllegalArgumentException e) {
      logger.warn(" error while retrieving {}", e.getMessage());
      return null;
    } catch (IOException e) {
      logger.warn(" error while retrieving {}", e.getMessage());
      return null;
    }
  }

  @Override
  public RetrievalObject retrieveNext()
  {
    if (fileData == null) {
      return null;
    }
    if (retrievalOffset == fileData.length) {
      retrievalOffset = 0;
      retrievalFile++;
      try {
        fileData = readData(new Path(baseDir + "/" + retrievalFile));
      } catch (IllegalArgumentException e) {
        logger.warn(" error while retrieving {}", e.getMessage());
        return null;
      } catch (IOException e) {
        logger.warn(" error while retrieving {}", e.getMessage());
        return null;
      }
    }
    byte[] nextIdentifier = new byte[8];
    longToByteArray(retrievalOffset, nextIdentifier, 0, 4);
    longToByteArray(retrievalFile, nextIdentifier, 4, 4);
    byte[] lengthArr = new byte[4];
    System.arraycopy(fileData, (int) retrievalOffset, lengthArr, 0, 4);
    int length = Ints.fromByteArray(lengthArr);
    RetrievalObject obj = new RetrievalObject();
    obj.setToken(byteArrayToLong(nextIdentifier, 7, 0));
    obj.setData(new byte[length]);
    System.arraycopy(fileData, (int) (retrievalOffset + 4), obj.getData(), 0, length);
    retrievalOffset += 4 + length;
    return obj;
  }

  @Override
  public boolean clean(long identifier)
  {
    byte[] b = new byte[8];
    longToByteArray(identifier, b, 0, 8);
    long cleanFileIndex = byteArrayToLong(b, 7, 4);
    if (cleanedFileCounter >= cleanFileIndex) {
      return true;
    }
    try {
      do {
        Path path = new Path(baseDir + "/" + cleanedFileCounter);
        if (fs.exists(path) && fs.isFile(path)) {
          fs.delete(path, false);
        }
        ++cleanedFileCounter;
      } while (cleanedFileCounter < cleanFileIndex);
    } catch (IOException e) {
      logger.warn("not able to close the streams {}", e.getMessage());
      return false;
    }
    return true;
  }

  private static final Logger logger = LoggerFactory.getLogger(HDFSStorage.class);

  @Override
  public boolean flush()
  {
    if (dataStream != null) {
      try {
        dataStream.close();
        filled = 0;
        ++fileCounter;
        return true;
      } catch (IOException ex) {
        logger.warn("not able to close the streams {}", ex.getMessage());
        return false;
      }
    }
    return true;
  }

}
