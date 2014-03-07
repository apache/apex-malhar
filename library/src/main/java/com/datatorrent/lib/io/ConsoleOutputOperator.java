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
package com.datatorrent.lib.io;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.Stateless;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Writes tuples to stdout of the container<p>
 * <br>
 * Mainly to be used for debugging. Users should be careful to not have this node listen to a high throughput stream<br>
 * <br>
 *
 * @since 0.3.2
 */
@Stateless
public class ConsoleOutputOperator extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(ConsoleOutputOperator.class);
  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    @SuppressWarnings("UseOfSystemOutOrSystemErr")
    public void process(Object t)
    {
      String s;
      if (stringFormat == null) {
        s = t.toString();
      }
      else {
        s = String.format(stringFormat, t);
      }
      if (!silent) {
        System.out.println(s);
      }
      if (debug) {
        logger.info(s);
      }
    }
  };
  public boolean silent = false;
  
  /**
   * @return the silent
   */
  public boolean isSilent()
  {
    return silent;
  }

  /**
   * @param silent the silent to set
   */
  public void setSilent(boolean silent)
  {
    this.silent = silent;
  }

  /**
   * When set to true, tuples are also logged at INFO level.
   */
  private boolean debug;
  /**
   * A formatter for {@link String#format}
   */
  private String stringFormat;

  public boolean isDebug()
  {
    return debug;
  }

  public void setDebug(boolean debug)
  {
    this.debug = debug;
  }

  public String getStringFormat()
  {
    return stringFormat;
  }

  public void setStringFormat(String stringFormat)
  {
    this.stringFormat = stringFormat;
  }
}
