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
package org.apache.apex.malhar.lib.io;

import java.io.PrintStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;

/**
 * Writes tuples to stdout of the container.
 * <p>
 * Mainly to be used for debugging. Users should be careful to not have this node listen to a high throughput stream<br>
 * <br>
 * </p>
 * @displayName Console Output
 * @category Output
 * @tags output operator
 *
 * @since 0.3.2
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
@Stateless
public class ConsoleOutputOperator extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(ConsoleOutputOperator.class);

  private transient PrintStream stream = isStderr() ? System.err : System.out;

  /**
   * This is the input port which receives the tuples that will be written to stdout.
   */
  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    @SuppressWarnings("UseOfSystemOutOrSystemErr")
    public void process(Object t)
    {
      String s;
      if (stringFormat == null) {
        s = t.toString();
      } else {
        s = String.format(stringFormat, t);
      }
      if (!silent) {
        stream.println(s);
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
   * When set to true, output to stderr
   */
  private boolean stderr = false;
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

  public boolean isStderr()
  {
    return stderr;
  }

  public void setStderr(boolean stderr)
  {
    this.stderr = stderr;
    stream = stderr ? System.err : System.out;
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
