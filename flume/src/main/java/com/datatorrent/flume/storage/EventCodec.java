/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.flume.storage;

import java.util.HashMap;

import org.apache.flume.Event;
import org.apache.flume.event.JSONEvent;
import org.apache.flume.event.SimpleEvent;

import com.datatorrent.lib.codec.KryoSerializableStreamCodec;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class EventCodec extends KryoSerializableStreamCodec<Event>
{
  public EventCodec()
  {
    super();
    register(Event.class);
    register(HashMap.class);
    register(SimpleEvent.class);
    register(JSONEvent.class);
  }

}
