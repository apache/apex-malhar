/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.helper;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.AttributeMap.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Stats.OperatorStats.CustomStats;

/**
 *
 */
public class OperatorContextTestHelper
{
  private final static ThreadLocal<DateFormat> DATE_FORMAT_THREAD_LOCAL = new ThreadLocal<DateFormat>()
  {
    @Override
    protected DateFormat initialValue()
    {
      return new SimpleDateFormat("yyyyMMddHHmmss");
    }
  };

  public static class TestIdOperatorContext extends TestContext implements OperatorContext
  {

    int id;
    String applicationPath;
    String applicationId;

    public TestIdOperatorContext(int id)
    {
      this.id = id;
    }

    public TestIdOperatorContext(String applicationId, String applicationPath, int operatorId)
    {
      this.id = operatorId;
      this.applicationId = applicationId;
      this.applicationPath = applicationPath;
    }

    @Override
    public int getId()
    {
      return id;
    }

    @Override
    public void setCustomStats(CustomStats stats)
    {
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getValue(Attribute<T> key)
    {
      if (key == DAG.APPLICATION_PATH) {
        return (T) applicationPath;
      }
      if (key == DAG.APPLICATION_ID) {
        return (T) applicationId;
      }
      return super.getValue(key);
    }

  }

  private static class TestContext implements Context
  {

    @Override
    public AttributeMap getAttributes()
    {
      return null;
    }

    @Override
    public <T> T getValue(Attribute<T> key)
    {
      return key.defaultValue;
    }

  }

  public static String getUniqueApplicationPath(String applicationPathPrefix)
  {
    Calendar calendar = Calendar.getInstance();
    return applicationPathPrefix + DATE_FORMAT_THREAD_LOCAL.get().format(calendar.getTime());
  }

}


