/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.flume.storage;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;

import com.datatorrent.api.Component;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class StorageDebugWrapper implements Storage, Configurable, Component<com.datatorrent.api.Context>
{
  HDFSStorage storage = new HDFSStorage();

  @Override
  public byte[] store(byte[] bytes)
  {
    byte[] ret = null;

    try {
      ret = storage.store(bytes);
    }
    finally {
      logger.info("storage.store(new byte[]{{}});", Arrays.toString(bytes));
      logger.debug("store {} {}", bytes, ret);
    }

    return ret;
  }

  @Override
  public byte[] retrieve(byte[] identifier)
  {
    byte[] ret = null;

    try {
      ret = storage.retrieve(identifier);
    }
    finally {
      logger.info("storage.retrieve(new byte[]{{}});", Arrays.toString(identifier));
      logger.debug("retrieve {} {}", identifier, ret);
    }

    return ret;
  }

  @Override
  public byte[] retrieveNext()
  {
    byte[] ret = null;
    try {
      ret = storage.retrieveNext();
    }
    finally {
      logger.info("storage.retrieveNext();");
      logger.debug("retrieveNext {}", ret);
    }

    return ret;
  }

  @Override
  public void clean(byte[] identifier)
  {
    try {
      storage.clean(identifier);
    }
    finally {
      logger.info("storage.clean(new byte[]{{}});", Arrays.toString(identifier));
      logger.debug("clean {}", identifier);
    }
  }

  @Override
  public void flush()
  {
    try {
      storage.flush();
    }
    finally {
      logger.info("storage.flush();");
      logger.debug("flush");
    }
  }

  @Override
  public void configure(Context cntxt)
  {
    try {
      storage.configure(cntxt);
    }
    finally {
      logger.info("configure {}", cntxt);
      logger.debug("configure {}", cntxt);
    }
  }

  @Override
  public void setup(com.datatorrent.api.Context t1)
  {
    try {
      storage.setup(t1);
    }
    finally {
      logger.info("setup {}", t1);
      logger.debug("setup {}", t1);
    }

  }

  @Override
  public void teardown()
  {
    try {
      storage.teardown();
    }
    finally {
      logger.info("storage.teardown();");
      logger.debug("teardown");
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(StorageDebugWrapper.class);
}
