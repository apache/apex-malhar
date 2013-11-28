/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.helper;

import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.AttributeMap.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Stats.OperatorStats.CustomStats;

/**
 *
 */
public class OperatorContextTestHelper
{

  public static class TestIdOperatorContext extends TestContext implements OperatorContext {

    int id;

    public TestIdOperatorContext(int id)
    {
      this.id = id;
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

  }

   private static class TestContext implements Context {

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

}


