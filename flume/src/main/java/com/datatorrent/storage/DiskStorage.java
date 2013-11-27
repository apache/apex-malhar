package com.datatorrent.storage;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

public class DiskStorage implements Storage
{

  private String baseDir;
  private long uniqueIdentifier;
  private long retrieveIdentifier;
  private File identityFile;
  private File retrivalFile;

  private DiskStorage(String baseDir, boolean restore) throws IOException
  {
    if (baseDir != null && baseDir.length() > 0) {
      File dir = new File(baseDir);
      if (!dir.isDirectory()) {
        throw new IOException("baseDir passed is not directory");
      }
      this.baseDir = baseDir;
      retrieveIdentifier = 1;
      uniqueIdentifier = 1;
      identityFile = new File(baseDir + "identityFile");
      retrivalFile = new File(baseDir + "retrivalFile");
      if (restore) {
        if (identityFile.exists() && identityFile.isFile()) {
          uniqueIdentifier = Long.valueOf(new String(Files.toByteArray(identityFile)));
        }
        if (retrivalFile.exists() && retrivalFile.isFile()) {
          retrieveIdentifier = Long.valueOf(new String(Files.toByteArray(retrivalFile)));
        }
      }
      Files.write(String.valueOf(uniqueIdentifier).getBytes(), identityFile);
      Files.write(String.valueOf(retrieveIdentifier).getBytes(), retrivalFile);

    } else {
      throw new IOException("filepath can't be null");
    }
  }

  public static Storage getInstance(String baseDir, boolean restore)
  {
    try {
      Storage storage = new DiskStorage(baseDir, restore);
      return storage;
    } catch (IOException ex) {
      logger.error("Not able to instantiate the stroage object {}", ex.getMessage());
    }
    return null;
  }

  public long store(byte[] bytes)
  {
    try {
      Files.write(bytes, new File(baseDir, String.valueOf(uniqueIdentifier)));
      long identifier = uniqueIdentifier;
      Files.write(String.valueOf(++uniqueIdentifier).getBytes(), identityFile);
      return identifier;
    } catch (IOException ex) {
      logger.warn("Error while storing the bytes {}", ex.getMessage());
      return -1;
    }

  }

  public RetrievalObject retrieve(long identifier)
  {
    /*
    if (identifier >= uniqueIdentifier) {
      return null;
    }
    File identityFile = new File(baseDir, String.valueOf(identifier));
    if (identityFile.exists() && identityFile.isFile()) {
      try {
        byte[] stored = Files.toByteArray(identityFile);
        retrieveIdentifier = ++identifier;
        Files.write(String.valueOf(retrieveIdentifier).getBytes(), retrivalFile);
        return stored;
      } catch (IOException ex) {
        logger.warn("Error while retrieving the bytes {}", ex.getMessage());
        return null;
      }
    } else {
      return null;
    }
    */
    return null;
  }

  public RetrievalObject retrieveNext()
  {
    return retrieve(retrieveIdentifier);
  }

  public boolean clean(long identifier)
  {
    File deletionFile = new File(baseDir, String.valueOf(identifier));
    if (deletionFile.exists() && deletionFile.isFile()) {
      return deletionFile.delete();
    }
    return false;
  }

  private static final Logger logger = LoggerFactory.getLogger(DiskStorage.class);

  @Override
  public boolean flush()
  {
    // TODO Auto-generated method stub
    return false;
  }

}
