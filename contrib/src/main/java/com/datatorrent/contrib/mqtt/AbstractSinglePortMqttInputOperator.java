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
package com.datatorrent.contrib.mqtt;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import org.fusesource.mqtt.client.Message;

/**
 * MQTT input adapter operator, which receives data from MQTT.<p><br>
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: No input port<br>
 * <b>Output</b>: Can have one output port<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * None<br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * <b>Benchmarks</b>:TBD
 * <br>
 *
 * @since 0.9.3
 */
public abstract class AbstractSinglePortMqttInputOperator<T> extends AbstractMqttInputOperator
{
  /**
   * the output port
   */
  @OutputPortFieldAnnotation(name = "out")
  final public transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();

  /**
   * Any concrete class derived from AbstractSinglePortMqttInputOperator has to implement this method
   * so that it knows what type of data it will receive from MQTT
   * It converts a MQTT Message into a Tuple. A Tuple can be of any type (derived from Java Object) that
   * operator user intends to.
   *
   * @param message
   */
  public abstract T getTuple(Message message);

  @Override
  public void emitTuple(Message message)
  {
    T t = getTuple(message);
    if (t != null) {
      outputPort.emit(t);
    }
  }

}
