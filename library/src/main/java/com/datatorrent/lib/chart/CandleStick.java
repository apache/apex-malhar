/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
 * <p>CandleStick class.</p>
 *
 * @since 0.3.2
 */
public class CandleStick<V extends Number> extends HighLow<V>
{
  private V open;
  private V close;

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
  public CandleStick(V o, V c, V h, V l)
  {
    super(h,l);
    open = o;
    close = c;
  }

  public void reset(V n)
  {
    open = close = high = low = n;
  }

  public V getOpen()
  {
    return open;
  }

  public V getClose()
  {
    return close;
  }

  public void setOpen(V o)
  {
    open = o;
  }

  public void setClose(V c)
  {
    close = c;
  }

}
