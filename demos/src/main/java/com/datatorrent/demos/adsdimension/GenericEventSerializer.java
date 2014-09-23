/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.adsdimension;

import com.google.common.collect.Maps;
import java.nio.ByteBuffer;
import java.util.Map;

public class GenericEventSerializer {

  static interface FieldSerializer {
    public void putField(ByteBuffer bb, Object o);
    public Object readField(ByteBuffer bb);
    public int dataLength();
  }

  static class IntSerializer implements FieldSerializer {
    @Override public void putField(ByteBuffer bb, Object o)
    {
      if (o == null)
        bb.putInt(0);
      else
        bb.putInt(((Integer) o).intValue());
    }

    @Override public Object readField(ByteBuffer bb)
    {
      int data = bb.getInt();
      return data;
    }

    @Override public int dataLength()
    {
      return 4;
    }
  }

  static class LongSerializer implements FieldSerializer {
    @Override public void putField(ByteBuffer bb, Object o)
    {
      if (o == null)
        bb.putLong(0);
      else
        bb.putLong(((Long) o).longValue());
    }

    @Override public Object readField(ByteBuffer bb)
    {
      long data = bb.getLong();
      return data;
    }

    @Override public int dataLength()
    {
      return 8;
    }
  }

  static class FloatSerializer implements FieldSerializer {
    @Override public void putField(ByteBuffer bb, Object o)
    {
      if (o == null)
        bb.putFloat(0.0f);
      else
        bb.putFloat(((Float) o).floatValue());
    }

    @Override public Object readField(ByteBuffer bb)
    {
      float data = bb.getFloat();
      return data;
    }

    @Override public int dataLength()
    {
      return 4;
    }
  }

  static class DoubleSerializer implements FieldSerializer {
    @Override public void putField(ByteBuffer bb, Object o)
    {
      if (o == null)
        bb.putDouble(0.0d);
      else
        bb.putDouble(((Double) o).doubleValue());
    }

    @Override public Object readField(ByteBuffer bb)
    {
      double data = bb.getDouble();
      return data;
    }

    @Override public int dataLength()
    {
      return 8;
    }
  }

  EventSchema eventDescription;

  // For kryo
  protected GenericEventSerializer() {}
  public GenericEventSerializer(EventSchema eventDescription)
  {
    this.eventDescription = eventDescription;
  }

  static Class stringToType(Class klass)
  {
    return klass;
  }

  static Map<Class, FieldSerializer> fieldSerializers = Maps.newHashMapWithExpectedSize(4);
  static {
    fieldSerializers.put(Integer.class, new IntSerializer());
    fieldSerializers.put(Float.class, new FloatSerializer());
    fieldSerializers.put(Long.class, new LongSerializer());
    fieldSerializers.put(Double.class, new DoubleSerializer());
  }

  byte[] getKey(MapAggregate event)
  {
    return getKey(event.keys);
  }

  byte[] getKey(Map<String, Object> tuple)
  {
    ByteBuffer bb = ByteBuffer.allocate(eventDescription.getKeyLen());

    bb.rewind();
    for (String key : eventDescription.keys) {
      Object o = tuple.get(key);
      fieldSerializers.get(eventDescription.getClass(key)).putField(bb, o);
    }
    bb.rewind();
    return bb.array();
  }

  byte[] getValue(MapAggregate event)
  {
    return getValue(event.fields);
  }

  byte[] getValue(Map<String, Object> tuple)
  {
    ByteBuffer bb = ByteBuffer.allocate(eventDescription.getValLen());
    for(String metric : eventDescription.getMetrices())
    {
      Object o = tuple.get(metric);
      fieldSerializers.get(eventDescription.getClass(metric)).putField(bb, o);
    }
    return bb.array();
  }

  public MapAggregate fromBytes(byte[] keyBytes, byte[] valBytes)
  {
    MapAggregate event = new MapAggregate(0);

    ByteBuffer bb = ByteBuffer.wrap(keyBytes);

    // Deserialize keys.
    for (java.lang.String key : eventDescription.keys) {
      java.lang.Object o = fieldSerializers.get(eventDescription.getClass(key)).readField(bb);
      event.keys.put(key, o);
    }

    // Deserialize metrics
    bb = ByteBuffer.wrap(valBytes);
    for(java.lang.String metric : eventDescription.getMetrices())
    {
      java.lang.Object o = fieldSerializers.get(eventDescription.getClass(metric)).readField(bb);
      event.fields.put(metric, o);
    }

    return event;
  }
}

