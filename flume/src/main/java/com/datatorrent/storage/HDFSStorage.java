package com.datatorrent.storage;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSStorage implements Storage
{
  private String baseDir;
  private long uniqueIdentifier;
  private long retrieveIdentifier;
  private Path identityFile;
  private Path retrivalFile;
  private FileSystem fs;

  private HDFSStorage(String baseDir, boolean restore) throws IOException
  {
    if (baseDir != null && baseDir.length() > 0) {
      Configuration conf = new Configuration();
      fs = FileSystem.get(conf);
      Path path = new Path(baseDir);
      if (!fs.exists(path) || !fs.isDirectory(path)) {
        throw new IOException("baseDir passed is not directory");
      }
      this.baseDir = baseDir;
      retrieveIdentifier = 1;
      uniqueIdentifier = 1;
      identityFile = new Path(baseDir + "/identityFile");
      retrivalFile = new Path(baseDir + "/retrivalFile");
      if (restore) {
        if (fs.exists(identityFile) && fs.isFile(identityFile)) {
          uniqueIdentifier = Long.valueOf(new String(readData(identityFile)));
        }
        if (fs.exists(retrivalFile) && fs.isFile(retrivalFile)) {
          retrieveIdentifier = Long.valueOf(new String(readData(retrivalFile)));
        }
      }
      writeData(identityFile, String.valueOf(uniqueIdentifier).getBytes());
      writeData(retrivalFile, String.valueOf(retrieveIdentifier).getBytes());
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

  private void writeData(Path path, byte[] data) throws IOException
  {
    FSDataOutputStream os = fs.create(path);
    os.write(data);
    os.close();
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

  public long store(byte[] bytes)
  {
    try {
      writeData(new Path(baseDir + "/" + String.valueOf(uniqueIdentifier)), bytes);
      long identifier = uniqueIdentifier;
      writeData(identityFile, String.valueOf(++uniqueIdentifier).getBytes());
      return identifier;
    } catch (IOException ex) {
      logger.warn("Error while storing the bytes {}", ex.getMessage());
      return -1;
    }

  }

  public byte[] retrieve(long identifier)
  {
    if (identifier >= uniqueIdentifier) {
      return null;
    }
    Path identityFile = new Path(baseDir + "/" + String.valueOf(identifier));
    try {
      if (fs.exists(identityFile) && fs.isFile(identityFile)) {
        byte[] stored = readData(identityFile);
        retrieveIdentifier = ++identifier;
        writeData(retrivalFile, String.valueOf(retrieveIdentifier).getBytes());
        return stored;
      } else {
        return null;
      }
    } catch (IOException ex) {
      logger.warn("Error while retrieving the bytes {}", ex.getMessage());
      return null;
    }
  }

  public byte[] retrieveNext()
  {
    return retrieve(retrieveIdentifier);
  }

  public boolean clean(long identifier)
  {
    Path deletionFile = new Path(baseDir + "/" + String.valueOf(identifier));
    try {
      if (fs.exists(deletionFile) && fs.isFile(deletionFile)) {
        return fs.delete(deletionFile, false);
      }
    } catch (IOException e) {
      logger.warn("error while deleting the identifier {}", e.getMessage());
    }
    return false;
  }

  private static final Logger logger = LoggerFactory.getLogger(HDFSStorage.class);

}
