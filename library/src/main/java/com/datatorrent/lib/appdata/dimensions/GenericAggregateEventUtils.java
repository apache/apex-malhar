/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.dimensions;

import com.datatorrent.lib.appdata.gpo.GPOByteArrayList;
import com.datatorrent.lib.appdata.gpo.GPOImmutable;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import java.nio.ByteBuffer;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GenericAggregateEventUtils
{
  private static ByteBuffer BB_2 = ByteBuffer.allocate(2);
  private static ByteBuffer BB_4 = ByteBuffer.allocate(4);
  private static ByteBuffer BB_8 = ByteBuffer.allocate(8);

  public static byte[] serialize(GenericAggregateEvent gae)
  {
    GPOByteArrayList bal = new GPOByteArrayList();
    byte[] keyBytes = GPOUtils.serialize(gae.getKeys());
    byte[] valueBytes = GPOUtils.serialize(gae.getAggregates());

    BB_4.putInt(gae.getSchemaID());
    bal.add(BB_4.array());
    BB_4.clear();

    BB_4.putInt(gae.getDimensionDescriptorID());
    bal.add(BB_4.array());
    BB_4.clear();

    BB_4.putInt(gae.getAggregatorIndex());
    bal.add(BB_4.array());
    BB_4.clear();
    
    bal.add(keyBytes);
    bal.add(valueBytes);

    return bal.elements();
  }

  public static GenericAggregateEvent deserialize(byte[] bytes,
                                                  FieldsDescriptor keyDescriptor,
                                                  FieldsDescriptor aggregateDescriptor)
  {
    GPOMutable gpoKeys = new GPOMutable(keyDescriptor);
    GPOMutable gpoAggregates = new GPOMutable(aggregateDescriptor);

    int offset = 0;

    BB_4.get(bytes, offset, 4);
    BB_4.rewind();
    int schemaID = BB_4.getInt();
    BB_4.clear();

    offset += 4;

    BB_4.get(bytes, offset, 4);
    BB_4.rewind();
    int dimensionDescriptorID = BB_4.getInt();
    BB_4.clear();

    offset += 4;

    BB_4.get(bytes, offset, 4);
    BB_4.rewind();
    int aggregatorID = BB_4.getInt();
    BB_4.clear();

    offset += 4;

    for(String field: keyDescriptor.getFields().getFields()) {
      Type type = keyDescriptor.getType(field);

      if(type == Type.BOOLEAN) {
        boolean val = bytes[offset] == (byte) 1;
        gpoKeys.setField(field, val);
        offset++;
      }
      else if(type == Type.BYTE) {
        byte val = bytes[offset];
        gpoKeys.setField(field, val);
        offset++;
      }
      else if(type == Type.SHORT) {
        BB_2.get(bytes, offset, 2);
        BB_2.rewind();
        short val = BB_2.getShort();
        BB_2.clear();
        gpoKeys.setField(field, val);
        offset += 2;
      }
      else if(type == Type.INTEGER) {
        BB_4.get(bytes, offset, 4);
        BB_4.rewind();
        int val = BB_4.getInt();
        BB_4.clear();
        gpoKeys.setField(field, val);
        offset += 4;
      }
      else if(type == Type.LONG) {
        BB_8.get(bytes, offset, 8);
        BB_8.rewind();
        long val = BB_8.getLong();
        BB_8.clear();
        gpoKeys.setField(field, val);
        offset += 8;
      }
      else if(type == Type.CHAR) {
        BB_2.get(bytes, offset, 2);
        BB_2.rewind();
        char val = BB_2.getChar();
        BB_2.clear();
        gpoKeys.setField(field, val);
        offset += 2;
      }
      else if(type == Type.STRING) {
        BB_4.get(bytes, offset, 4);
        BB_4.rewind();
        int val = BB_4.getInt();
        BB_4.clear();
        gpoKeys.setField(field, val);
        offset += 4;
      }
    }

    for(String field: aggregateDescriptor.getFields().getFields()) {
      Type type = aggregateDescriptor.getType(field);

      if(type == Type.BOOLEAN) {
        boolean val = bytes[offset] == (byte) 1;
        gpoAggregates.setField(field, val);
        offset++;
      }
      else if(type == Type.BYTE) {
        byte val = bytes[offset];
        gpoAggregates.setField(field, val);
        offset++;
      }
      else if(type == Type.SHORT) {
        BB_2.get(bytes, offset, 2);
        BB_2.rewind();
        short val = BB_2.getShort();
        BB_2.clear();
        gpoAggregates.setField(field, val);
        offset += 2;
      }
      else if(type == Type.INTEGER) {
        BB_4.get(bytes, offset, 4);
        BB_4.rewind();
        int val = BB_4.getInt();
        BB_4.clear();
        gpoAggregates.setField(field, val);
        offset += 4;
      }
      else if(type == Type.LONG) {
        BB_8.get(bytes, offset, 8);
        BB_8.rewind();
        long val = BB_8.getLong();
        BB_8.clear();
        gpoAggregates.setField(field, val);
        offset += 8;
      }
      else if(type == Type.CHAR) {
        BB_2.get(bytes, offset, 2);
        BB_2.rewind();
        char val = BB_2.getChar();
        BB_2.clear();
        gpoAggregates.setField(field, val);
        offset += 2;
      }
      else if(type == Type.STRING) {
        BB_4.get(bytes, offset, 4);
        BB_4.rewind();
        int val = BB_4.getInt();
        BB_4.clear();
        gpoAggregates.setField(field, val);
        offset += 4;
      }
    }

    return new GenericAggregateEvent(new GPOImmutable(gpoKeys),
                                     gpoAggregates,
                                     schemaID,
                                     dimensionDescriptorID,
                                     aggregatorID);
  }
}
