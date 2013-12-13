package com.datatorrent.flume.storage;

public interface Storage
{
  /**
   * This stores the bytes and returns the unique identifier to retrieve these bytes
   *
   * @param bytes
   * @return
   */
  long store(byte[] bytes);

  /**
   * This returns the bytes and the identifier identified by the identifier. This identifier is used to retrieve next bytes in retriveNext function call
   *
   * @param identifier
   * @return
   */
  RetrievalObject retrieve(long identifier);

  /**
   * This returns the next identifier and bytes identified by this identifier
   *
   * @return
   */
  RetrievalObject retrieveNext();

  /**
   * This is used to clean up of the bytes identified by identifier
   *
   * @param identifier
   * @return
   */
  boolean clean(long identifier);

  /**
   * This flushes the data from stream
   *
   * @return
   */
  boolean flush();

  /**
   * This flushes the data and closes stream
   *
   * @return
   */
  boolean close();

}
