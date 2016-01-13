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
package com.datatorrent.lib.expressions;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * This class contains some public static method which are utility methods available in expression to be invoked.
 *
 * This set of methods takes care of some basic functionality associated with date/time.
 */
public class DateTimeUtils
{
  /**
   * Returns Date object representing current datetime.
   */
  public Date nowDate()
  {
    return new Date();
  }

  /**
   * Returns current time in unix time format.
   */
  public long nowTime()
  {
    return System.currentTimeMillis();
  }

  /**
   * Returns Calender instance representing current datetime for default timezone.
   */
  public Calendar nowCalender()
  {
    return Calendar.getInstance();
  }

  /**
   * Returns Calender instance of current datetime for given timezone.
   * @param timeZone Timezone for which Calender instance needs to be returned.
   */
  public Calendar nowCalender(String timeZone)
  {
    return Calendar.getInstance(TimeZone.getTimeZone(timeZone));
  }
}
