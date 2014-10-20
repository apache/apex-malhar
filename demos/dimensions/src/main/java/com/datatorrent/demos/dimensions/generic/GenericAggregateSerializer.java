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
package com.datatorrent.demos.dimensions.generic;

import com.google.common.collect.Maps;


import java.nio.ByteBuffer;
import java.util.Map;

public class GenericAggregateSerializer {


  EventSchema eventSchema;
  static Map<Class<?>, FieldSerializer> fieldSerializers = Maps.newHashMap();

  // For kryo
  protected GenericAggregateSerializer() {}
  public GenericAggregateSerializer(EventSchema eventSchema)
  {
    this.eventSchema = eventSchema;
  }

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
        bb.putInt(((Number) o).intValue());
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
        bb.putLong(0L);
      else
        bb.putLong(((Number) o).longValue());
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
        bb.putFloat(((Number) o).floatValue());
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
        bb.putDouble(((Number) o).doubleValue());
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

  static {
    fieldSerializers.put(Integer.class, new IntSerializer());
    fieldSerializers.put(Float.class, new FloatSerializer());
    fieldSerializers.put(Long.class, new LongSerializer());
    fieldSerializers.put(Double.class, new DoubleSerializer());
  }

  byte[] getKey(GenericAggregate event)
  {
    ByteBuffer bb = ByteBuffer.allocate(eventSchema.getKeyLen());

    // Write timestamp as first field
    fieldSerializers.get(eventSchema.getClass(eventSchema.getTimestamp())).putField(bb, event.timestamp);
    // Write event keys
    for (int i=0; i < eventSchema.genericEventKeys.size(); i++) {
      String keyName = eventSchema.genericEventKeys.get(i);
      Object keyValue = event.keys[i];
      fieldSerializers.get(eventSchema.getClass(keyName)).putField(bb, keyValue);
    }
    return bb.array();
  }

  byte[] getValue(GenericAggregate event)
  {
    ByteBuffer bb = ByteBuffer.allocate(eventSchema.getValLen());
    for (int i=0; i < eventSchema.genericEventValues.size(); i++) {
      String aggregateName = eventSchema.genericEventValues.get(i);
      Object aggregateValue = event.aggregates[i];
      fieldSerializers.get(eventSchema.getClass(aggregateName)).putField(bb, aggregateValue);
    }
    return bb.array();
  }

  public GenericAggregate fromBytes(byte[] keyBytes, byte[] valBytes)
  {
    GenericAggregate event = new GenericAggregate();
    event.keys = new Object[eventSchema.genericEventKeys.size()];
    event.aggregates = new Object[eventSchema.genericEventValues.size()];

    ByteBuffer bb = ByteBuffer.wrap(keyBytes);

    // Deserialize timestamp
    event.timestamp = (Long)fieldSerializers.get(eventSchema.getClass(eventSchema.getTimestamp())).readField(bb);

    // Deserialize keys
    for (int i=0; i < eventSchema.genericEventKeys.size(); i++) {
      String keyName = eventSchema.genericEventKeys.get(i);
      event.keys[i] = fieldSerializers.get(eventSchema.getClass(keyName)).readField(bb);
    }

    // Deserialize aggregate data
    bb = ByteBuffer.wrap(valBytes);
    for (int i=0; i < eventSchema.genericEventValues.size(); i++) {
      String aggregateName = eventSchema.genericEventValues.get(i);
      event.aggregates[i] = fieldSerializers.get(eventSchema.getClass(aggregateName)).readField(bb);
    }
    return event;
  }
}

