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
package org.apache.apex.malhar.lib.helper;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Attribute.AttributeMap;
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

  public static OperatorContext mockOperatorContext(int id)
  {
    return mockOperatorContext(id, null);
  }


  public static OperatorContext mockOperatorContext(int id, final AttributeMap map)
  {
    OperatorContext context = Mockito.mock(OperatorContext.class);
    Mockito.when(context.getId()).thenReturn(id);
    Mockito.when(context.getAttributes()).thenReturn(map);
    Mockito.doThrow(new UnsupportedOperationException("not supported")).when(context).sendMetrics(Mockito.<Collection<String>>any());
    Mockito.doThrow(new UnsupportedOperationException("not supported")).when(context).setCounters(Mockito.any());
    Mockito.when(context.getValue(Mockito.<Attribute>any())).thenAnswer(new Answer<Object>()
    {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable
      {
        final Attribute key = (Attribute)invocation.getArguments()[0];
        Object value = map.get(key);
        if (value != null) {
          return value;
        }
        return key.defaultValue;
      }
    });
    Mockito.doNothing().when(context).setCounters(Mockito.any());
    Mockito.when(context.getWindowsFromCheckpoint()).thenReturn(0);
    return context;
  }

  private static class TestContext implements Context
  {
    @Override
    public Attribute.AttributeMap getAttributes()
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
