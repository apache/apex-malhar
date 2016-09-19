/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.lib.helper;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;

import javax.annotation.Nonnull;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;

/**
 *
 */
public class OperatorContextTestHelper
{
  private static final ThreadLocal<DateFormat> DATE_FORMAT_THREAD_LOCAL = new ThreadLocal<DateFormat>()
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
    com.datatorrent.api.Attribute.AttributeMap attributes;

    public TestIdOperatorContext(int id)
    {
      this.id = id;
    }

    public TestIdOperatorContext(int id, @Nonnull com.datatorrent.api.Attribute.AttributeMap map)
    {
      this.id = id;
      this.attributes = map;
    }

    @Override
    public int getId()
    {
      return id;
    }

    @Override
    public void sendMetrics(Collection<String> metricNames)
    {
      throw new UnsupportedOperationException("not supported");
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getValue(Attribute<T> key)
    {
      T value = attributes.get(key);
      if (value != null) {
        return value;
      }
      return super.getValue(key);
    }

    @Override
    public void setCounters(Object counters)
    {
      /* intentionally no-op */
    }

    @Override
    public int getWindowsFromCheckpoint()
    {
      return 0;
    }
  }

  private static class TestContext implements Context
  {
    @Override
    public com.datatorrent.api.Attribute.AttributeMap getAttributes()
    {
      return null;
    }

    @Override
    public <T> T getValue(Attribute<T> key)
    {
      return key.defaultValue;
    }

    @Override
    public void setCounters(Object counters)
    {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void sendMetrics(Collection<String> metricNames)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

  }

  public static String getUniqueApplicationPath(String applicationPathPrefix)
  {
    Calendar calendar = Calendar.getInstance();
    return applicationPathPrefix + DATE_FORMAT_THREAD_LOCAL.get().format(calendar.getTime());
  }

}
