/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.flume.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;

import com.datatorrent.api.Component;
import com.datatorrent.netlet.util.Slice;

/**
 * <p>DebugWrapper class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.9.4
 */
public class DebugWrapper implements Storage, Configurable, Component<com.datatorrent.api.Context>
{
  HDFSStorage storage = new HDFSStorage();

  @Override
  public byte[] store(Slice bytes)
  {
    byte[] ret = null;

    try {
      ret = storage.store(bytes);
    } finally {
      logger.debug("storage.store(new byte[]{{}});", bytes);
    }

    return ret;
  }

  @Override
  public byte[] retrieve(byte[] identifier)
  {
    byte[] ret = null;

    try {
      ret = storage.retrieve(identifier);
    } finally {
      logger.debug("storage.retrieve(new byte[]{{}});", identifier);
    }

    return ret;
  }

  @Override
  public byte[] retrieveNext()
  {
    byte[] ret = null;
    try {
      ret = storage.retrieveNext();
    } finally {
      logger.debug("storage.retrieveNext();");
    }

    return ret;
  }

  @Override
  public void clean(byte[] identifier)
  {
    try {
      storage.clean(identifier);
    } finally {
      logger.debug("storage.clean(new byte[]{{}});", identifier);
    }
  }

  @Override
  public void flush()
  {
    try {
      storage.flush();
    } finally {
      logger.debug("storage.flush();");
    }
  }

  @Override
  public void configure(Context cntxt)
  {
    try {
      storage.configure(cntxt);
    } finally {
      logger.debug("storage.configure({});", cntxt);
    }
  }

  @Override
  public void setup(com.datatorrent.api.Context t1)
  {
    try {
      storage.setup(t1);
    } finally {
      logger.debug("storage.setup({});", t1);
    }

  }

  @Override
  public void teardown()
  {
    try {
      storage.teardown();
    } finally {
      logger.debug("storage.teardown();");
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(DebugWrapper.class);
}
