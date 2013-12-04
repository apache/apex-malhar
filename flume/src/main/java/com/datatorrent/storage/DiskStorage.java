package com.datatorrent.storage;

import java.io.File;
import java.io.IOException;

import com.google.common.io.Files;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;

public class DiskStorage implements Storage, Configurable
{
  private String baseDir;
  private long uniqueIdentifier;
  private long retrieveIdentifier;
  private File identityFile;
  private File retrivalFile;

  @Override
  public void configure(Context context)
  {
    baseDir = context.getString("baseDir", "/tmp");
    boolean restore = context.getBoolean("restore", false);
    logger.debug("baseDir = {}\nrestore = {}", baseDir, restore);
    
    File dir = new File(baseDir);
    if (!dir.isDirectory()) {
      throw new Error(baseDir + "passed as baseDir is not a directory");
    }
    retrieveIdentifier = 1;
    uniqueIdentifier = 1;
    identityFile = new File(baseDir + "identityFile");
    retrivalFile = new File(baseDir + "retrivalFile");
    try {
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
    }
    catch (IOException io) {
      throw new RuntimeException(io);
    }
  }

  @Override
  public long store(byte[] bytes)
  {
    try {
      Files.write(bytes, new File(baseDir, String.valueOf(uniqueIdentifier)));
      long identifier = uniqueIdentifier;
      Files.write(String.valueOf(++uniqueIdentifier).getBytes(), identityFile);
      return identifier;
    }
    catch (IOException ex) {
      logger.warn("Error while storing the bytes {}", ex.getMessage());
      return -1;
    }

  }

  @Override
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

  @Override
  public RetrievalObject retrieveNext()
  {
    return retrieve(retrieveIdentifier);
  }

  @Override
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
