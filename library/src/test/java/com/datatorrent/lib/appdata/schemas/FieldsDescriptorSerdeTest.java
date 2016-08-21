package com.datatorrent.lib.appdata.schemas;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;

import com.datatorrent.netlet.util.Slice;

/**
 * Created by tfarkas on 8/21/16.
 */
public class FieldsDescriptorSerdeTest
{
  @Test
  public void simpleSerdeTest()
  {
    Map<String, Type> fieldToType = Maps.newHashMap();

    fieldToType.put("a", Type.BOOLEAN);
    fieldToType.put("b", Type.BYTE);
    fieldToType.put("c", Type.CHAR);
    fieldToType.put("d", Type.DOUBLE);
    fieldToType.put("e", Type.FLOAT);
    fieldToType.put("f", Type.INTEGER);
    fieldToType.put("g", Type.LONG);

    FieldsDescriptor fd = new FieldsDescriptor(fieldToType);

    FieldsDescriptorSerde fdSerde = new FieldsDescriptorSerde();

    Slice fdSlice = fdSerde.serialize(fd);

    FieldsDescriptor clonedFd = fdSerde.deserialize(fdSlice);

    Assert.assertEquals(clonedFd, fd);
  }
}
