package com.datatorrent.flume.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flume.Event;

import com.datatorrent.netlet.util.Slice;

/**
 * <p>ErrorMaskingEventCodec class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 1.0.4
 */
public class ErrorMaskingEventCodec extends EventCodec
{

  @Override
  public Object fromByteArray(Slice fragment)
  {
    try {
      return super.fromByteArray(fragment);
    } catch (RuntimeException re) {
      logger.warn("Cannot deserialize event {}", fragment, re);
    }

    return null;
  }

  @Override
  public Slice toByteArray(Event event)
  {
    try {
      return super.toByteArray(event);
    } catch (RuntimeException re) {
      logger.warn("Cannot serialize event {}", event, re);
    }

    return null;
  }


  private static final Logger logger = LoggerFactory.getLogger(ErrorMaskingEventCodec.class);
}
