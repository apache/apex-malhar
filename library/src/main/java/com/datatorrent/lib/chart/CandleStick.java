/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.chart;

import com.datatorrent.lib.util.HighLow;

/**
 *
 */
public class CandleStick extends HighLow
{
  private Number open;
  private Number close;

  /**
   * Added default constructor for deserializer.
   */
  public CandleStick()
  {
    super();
    open = close = null;
  }

  /**
   * Constructor
   *
   * @param o
   * @param c
   * @param h
   * @param l
   */
  public CandleStick(Number o, Number c, Number h, Number l)
  {
    super(h,l);
    open = o;
    close = c;
  }

  public void reset(Number n)
  {
    open = close = high = low = n;
  }

  public Number getOpen()
  {
    return open;
  }

  public Number getClose()
  {
    return close;
  }

  public void setOpen(Number o)
  {
    open = o;
  }

  public void setClose(Number c)
  {
    close = c;
  }

}
