/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.flume.storage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.netlet.util.Slice;

/**
 * <p>EventCodec class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.9.4
 */
public class EventCodec implements StreamCodec<Event>
{
  private final transient Kryo kryo;

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
    HashMap<String, String> headers = kryo.readObjectOrNull(input, HashMap.class);
    byte[] body = kryo.readObjectOrNull(input, byte[].class);
    return EventBuilder.withBody(body, headers);
  }

  @Override
  public Slice toByteArray(Event event)
  {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    Output output = new Output(os);

    Map<String, String> headers = event.getHeaders();
    if (headers != null && headers.getClass() != HashMap.class) {
      HashMap<String, String> tmp = new HashMap<String, String>(headers.size());
      tmp.putAll(headers);
      headers = tmp;
    }
    kryo.writeObjectOrNull(output, headers, HashMap.class);
    kryo.writeObjectOrNull(output, event.getBody(), byte[].class);
    output.flush();
    final byte[] bytes = os.toByteArray();
    return new Slice(bytes, 0, bytes.length);
  }

  @Override
  public int getPartition(Event o)
  {
    return o.hashCode();
  }

  private static final Logger logger = LoggerFactory.getLogger(EventCodec.class);
}
