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
package org.apache.apex.malhar.lib.io.jms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;

/**
 * This is the base implementation of a single port JMS output operator.&nbsp;
 * A concrete operator should be created from this skeleton implementation.
 * <p>
 * Ports:<br>
 * <b>Input</b>: Have only one input port<br>
 * <b>Output</b>: No output port<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * Class derived from this has to implement the abstract method createMessage() <br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * Benchmarks:<br>
 * TBD<br>
 * <br>
 * </p>
 * @displayName Abstract JMS Single Port Output
 * @category Messaging
 * @tags jms, output operator
 *
 * @since 0.3.2
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class AbstractJMSSinglePortOutputOperator<T> extends AbstractJMSOutputOperator
{
  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(AbstractJMSSinglePortOutputOperator.class);

  /**
   * Convert to and send message.
   * @param tuple
   */
  protected void processTuple(T tuple)
  {
    sendMessage(tuple);
  }

  /**
   * This is an input port which receives tuples to be written out to an JMS message bus.
   */
  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      processTuple(tuple);
    }
  };
}
