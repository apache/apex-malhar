/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.flume.storage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

import com.datatorrent.api.StreamCodec;

import com.datatorrent.common.util.Slice;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class EventCodec implements StreamCodec<Event>
{
  private transient final Kryo kryo;

  public EventCodec()
  {
    this.kryo = new Kryo();
    this.kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
  }

  @Override
  public Object fromByteArray(Slice fragment)
  {
    ByteArrayInputStream is = new ByteArrayInputStream(fragment.buffer, fragment.offset, fragment.length);
    Input input = new Input(is);

    @SuppressWarnings("unchecked")
    HashMap<String, String> headers = kryo.readObject(input, HashMap.class);
    byte[] body = kryo.readObject(input, byte[].class);
    return EventBuilder.withBody(body, headers);
  }

  @Override
  public Slice toByteArray(Event event)
  {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    Output output = new Output(os);
    kryo.writeObject(output, event.getHeaders());
    kryo.writeObject(output, event.getBody());
    output.flush();
    return new Slice(os.toByteArray(), 0, os.toByteArray().length);
  }

  @Override
  public int getPartition(Event o)
  {
    return o.hashCode();
  }

}
