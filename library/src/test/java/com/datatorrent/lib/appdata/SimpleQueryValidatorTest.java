/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata;

import com.datatorrent.lib.appdata.qr.DataType;
import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.DataDeserializerInfo;
import com.datatorrent.lib.appdata.qr.DataValidatorInfo;
import com.datatorrent.lib.appdata.qr.SimpleDataDeserializer;
import com.datatorrent.lib.appdata.qr.SimpleDataValidator;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class SimpleQueryValidatorTest
{
  @Test
  public void testValidatingQuery()
  {
    TestQuery testQuery = new TestQuery();
    SimpleDataValidator sqv = new SimpleDataValidator();

    Assert.assertFalse("The query object is not valid.", sqv.validate(testQuery));
  }

  @DataType(type = TestQuery.TYPE)
  @DataDeserializerInfo(clazz = SimpleDataDeserializer.class)
  @DataValidatorInfo(clazz = SimpleDataValidator.class)
  public static class TestQuery extends Query
  {
    public static final String TYPE = "testQuery";

    public TestQuery()
    {
    }
  }
}
