/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.storage;

import com.datatorrent.netlet.util.Slice;

/**
 * <p>Storage interface.</p>
 *
 * @author Gaurav Gupta  <gaurav@datatorrent.com>
 * @since 0.9.2
 */
public interface Storage
{
  /**
   * key in the context for Unique identifier for the storage which may be used to recover from failure.
   */
  String ID = "id";

  /**
   * This stores the bytes and returns the unique identifier to retrieve these bytes
   *
   * @param bytes
   * @return
   */
  byte[] store(Slice bytes);

  /**
   * This returns the data bytes for the current identifier and the identifier for next data bytes. <br/>
   * The first eight bytes contain the identifier and the remaining bytes contain the data
   *
   * @param identifier
   * @return
   */
  byte[] retrieve(byte[] identifier);

  /**
   * This returns data bytes and the identifier for the next data bytes. The identifier for current data bytes is based
   * on the retrieve method call and number of retrieveNext method calls after retrieve method call. <br/>
   * The first eight bytes contain the identifier and the remaining bytes contain the data
   *
   * @return
   */
  byte[] retrieveNext();

  /**
   * This is used to clean up the files identified by identifier
   *
   * @param identifier
   */
  void clean(byte[] identifier);

  /**
   * This flushes the data from stream
   *
   */
  void flush();

}
