/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata;

import com.datatorrent.lib.appdata.qr.QRType;
import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.QueryDeserializerInfo;
import com.datatorrent.lib.appdata.qr.QueryValidatorInfo;
import com.datatorrent.lib.appdata.qr.SimpleQueryDeserializer;
import com.datatorrent.lib.appdata.qr.SimpleQueryValidator;
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
    SimpleQueryValidator sqv = new SimpleQueryValidator();

    Assert.assertFalse("The query object is not valid.", sqv.validate(testQuery));
  }

  @QRType(type = TestQuery.TYPE)
  @QueryDeserializerInfo(clazz = SimpleQueryDeserializer.class)
  @QueryValidatorInfo(clazz = SimpleQueryValidator.class)
  public static class TestQuery extends Query
  {
    public static final String TYPE = "testQuery";

    public TestQuery()
    {
    }
  }
}
