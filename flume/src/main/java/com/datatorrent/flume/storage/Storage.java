package com.datatorrent.flume.storage;

public interface Storage
{
  /**
   * This stores the bytes and returns the unique identifier to retrieve these bytes
   *
   * @param bytes
   * @return
   */
  byte[] store(byte[] bytes);

  /**
   * This returns the bytes and the identifier identified by the identifier. This identifier is used to retrieve next bytes in retriveNext function call
   *
   * @param identifier
   * @return
   */
  byte[] retrieve(byte[] identifier);

  /**
   * This returns the next identifier and bytes identified by this identifier
   *
   * @return
   */
  byte[] retrieveNext();

  /**
   * This is used to clean up of the bytes identified by identifier
   *
   * @param identifier
   * @return
   */
  void clean(byte[] identifier);

  /**
   * This flushes the data from stream
   *
   * @return
   */
  void flush();

  /**
   * This flushes the data and closes stream
   *
   * @return
   */
  void close();

}
