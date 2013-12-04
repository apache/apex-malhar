package com.datatorrent.flume.storage;

public interface Storage
{
  /**
   * This stores the bytes and returns the unique identifier to retrieve these bytes
   *
   * @param bytes
   * @return
   */
  public long store(byte[] bytes);

  /**
   * This returns the bytes identified by the identifier
   *
   * @param identifier
   * @return
   */
  public RetrievalObject retrieve(long identifier);

  /**
   * This returns the the bytes
   *
   * @return
   */
  public RetrievalObject retrieveNext();

  /**
   * This is used to clean up of the bytes identified by identifier
   *
   * @param identifier
   * @return
   */
  public boolean clean(long identifier);

  /**
   * This flushes the data from stream
   * @return
   */
  public boolean flush();
  
  /**
   * This flushes the data and closes stream
   * @return
   */
  public boolean close();

}
