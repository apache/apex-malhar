package com.datatorrent.lib.appdata.schemas;

import java.util.Map;

import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.commons.lang3.mutable.MutableInt;

import com.google.common.collect.Maps;

import com.datatorrent.lib.appdata.gpo.GPOByteArrayList;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.netlet.util.Slice;

/**
 * Created by tfarkas on 8/20/16.
 */
public class FieldsDescriptorSerde implements Serde<FieldsDescriptor, Slice>
{
  private final GPOByteArrayList bytesList = new GPOByteArrayList();

  @Override
  public Slice serialize(FieldsDescriptor fd)
  {
    Map<String, Type> fieldToType = fd.getFieldToType();

    byte[] sizeBytes = GPOUtils.serializeInt(fieldToType.size());
    bytesList.add(sizeBytes);

    for (Map.Entry<String, Type> entry: fieldToType.entrySet()) {
      String fieldName = entry.getKey();
      String typeName = entry.getValue().getName();

      byte[] fieldNameBytes = GPOUtils.serializeString(fieldName);
      byte[] typeNameBytes = GPOUtils.serializeString(typeName);

      bytesList.add(fieldNameBytes);
      bytesList.add(typeNameBytes);
    }

    byte[] bytes = bytesList.toByteArray();
    bytesList.clear();

    return new Slice(bytes);
  }

  @Override
  public FieldsDescriptor deserialize(Slice slice, MutableInt offset)
  {
    offset.add(slice.offset);
    int size = GPOUtils.deserializeInt(slice.buffer, offset);

    Map<String, Type> fieldToType = Maps.newHashMap();

    for (int counter = 0; counter < size; counter++) {
      String key = GPOUtils.deserializeString(slice.buffer, offset);
      String value = GPOUtils.deserializeString(slice.buffer, offset);

      fieldToType.put(key, Type.getTypeEx(value));
    }

    return new FieldsDescriptor(fieldToType);
  }

  @Override
  public FieldsDescriptor deserialize(Slice slice)
  {
    return deserialize(slice, new MutableInt(0));
  }
}
