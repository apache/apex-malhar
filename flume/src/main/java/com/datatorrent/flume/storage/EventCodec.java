/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.flume.storage;

import java.util.HashMap;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

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
    kryo.addDefaultSerializer(Event.class, new EventSerializer());
  }

  public static class EventSerializer extends Serializer<Event>
  {
    @Override
    public void write(Kryo kryo, Output output, Event object)
    {
      kryo.writeObject(output, object.getHeaders());
      kryo.writeObject(output, object.getBody());
    }

    @Override
    public Event read(Kryo kryo, Input input, Class<Event> type)
    {
      @SuppressWarnings("unchecked")
      HashMap<String, String> headers = kryo.readObject(input, HashMap.class);
      byte[] body = kryo.readObject(input, byte[].class);
      return EventBuilder.withBody(body, headers);
    }

  }

}
