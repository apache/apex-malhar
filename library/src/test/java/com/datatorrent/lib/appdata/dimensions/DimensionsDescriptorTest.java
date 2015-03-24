/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.dimensions;

import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class DimensionsDescriptorTest
{
  @Test
  public void simpleTest1()
  {
    TestDimensionsSchema tds = new TestDimensionsSchema();
    DimensionsDescriptor ad = new DimensionsDescriptor(TestDimensionsSchema.KEY_1_NAME);

    Set<String> fields = Sets.newHashSet();
    fields.add(TestDimensionsSchema.KEY_1_NAME);

    Assert.assertEquals("The fields should match.", fields, ad.getFields().getFields());
    Assert.assertEquals("The timeunit should be null.", null, ad.getTimeBucket());
  }

  @Test
  public void simpleTest2()
  {
    TestDimensionsSchema tds = new TestDimensionsSchema();
    DimensionsDescriptor ad = new DimensionsDescriptor(TestDimensionsSchema.KEY_1_NAME +
                                                       DimensionsDescriptor.DELIMETER_SEPERATOR +
                                                       TestDimensionsSchema.KEY_2_NAME);

    Set<String> fields = Sets.newHashSet();
    fields.add(TestDimensionsSchema.KEY_1_NAME);
    fields.add(TestDimensionsSchema.KEY_2_NAME);

    Assert.assertEquals("The fields should match.", fields, ad.getFields().getFields());
    Assert.assertEquals("The timeunit should be null.", null, ad.getTimeBucket());
  }

  @Test
  public void simpleTimeTest()
  {
    TestDimensionsSchema tds = new TestDimensionsSchema();
    DimensionsDescriptor ad = new DimensionsDescriptor(TestDimensionsSchema.KEY_1_NAME +
                                                       DimensionsDescriptor.DELIMETER_SEPERATOR +
                                                       DimensionsDescriptor.DIMENSION_TIME +
                                                       DimensionsDescriptor.DELIMETER_EQUALS +
                                                       "DAYS");

    Set<String> fields = Sets.newHashSet();
    fields.add(TestDimensionsSchema.KEY_1_NAME);
    fields.add(DimensionsDescriptor.DIMENSION_TIME);

    Assert.assertEquals("The fields should match.", fields, ad.getFields().getFields());
    Assert.assertEquals("The timeunit should be DAYS.", TimeUnit.DAYS, ad.getTimeBucket());
  }

  public static class TestDimensionsSchema implements DimensionsSchema
  {
    public static final String KEY_1_NAME = "key1";
    public static final Type KEY_1_TYPE = Type.INTEGER;
    public static final String KEY_2_NAME = "key2";
    public static final Type KEY_2_TYPE = Type.STRING;

    public static final String AGG_1_NAME = "agg1";
    public static final Type AGG_1_TYPE = Type.INTEGER;
    public static final String AGG_2_NAME = "agg2";
    public static final Type AGG_2_TYPE = Type.STRING;

    private FieldsDescriptor keyFieldDescriptor;
    private FieldsDescriptor aggregateDescriptor;
    private FieldsDescriptor allDescriptor;

    public TestDimensionsSchema()
    {
      Map<String, Type> keyFieldToType = Maps.newHashMap();
      keyFieldToType.put(KEY_1_NAME, KEY_1_TYPE);
      keyFieldToType.put(KEY_2_NAME, KEY_2_TYPE);

      Map<String, Type> aggregateFieldToType = Maps.newHashMap();
      aggregateFieldToType.put(AGG_1_NAME, AGG_1_TYPE);
      aggregateFieldToType.put(AGG_1_NAME, AGG_1_TYPE);

      Map<String, Type> allFieldToType = Maps.newHashMap();

      for(Map.Entry<String, Type> entry: keyFieldToType.entrySet()) {
        allFieldToType.put(entry.getKey(), entry.getValue());
      }

      for(Map.Entry<String, Type> entry: aggregateFieldToType.entrySet()) {
        allFieldToType.put(entry.getKey(), entry.getValue());
      }

      keyFieldDescriptor = new FieldsDescriptor(keyFieldToType);
      aggregateDescriptor = new FieldsDescriptor(aggregateFieldToType);
      allDescriptor = new FieldsDescriptor(allFieldToType);
    }

    @Override
    public FieldsDescriptor getKeyFieldDescriptor()
    {
      return keyFieldDescriptor;
    }

    @Override
    public FieldsDescriptor getAggregateFieldDescriptor()
    {
      return aggregateDescriptor;
    }

    @Override
    public FieldsDescriptor getAllFieldsDescriptor()
    {
      return allDescriptor;
    }
  }
}
