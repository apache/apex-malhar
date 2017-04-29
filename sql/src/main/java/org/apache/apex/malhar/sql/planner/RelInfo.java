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
package org.apache.apex.malhar.sql.planner;

import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Operator;

/**
 * This object communicates stream and connection data between various stages of relational algebra.
 *
 * @since 3.6.0
 */
@InterfaceStability.Evolving
public class RelInfo
{
  private List<Operator.InputPort> inputPorts;
  private Operator operator;
  private Operator.OutputPort outPort;
  private RelDataType outRelDataType;
  private Class clazz;
  private String relName;

  public RelInfo(String relName, List<Operator.InputPort> inputPorts, Operator operator, Operator.OutputPort outPort,
      RelDataType outRelDataType)
  {
    this.inputPorts = inputPorts;
    this.relName = relName;
    this.operator = operator;
    this.outPort = outPort;
    this.outRelDataType = outRelDataType;
    this.clazz = null;
  }

  public RelInfo(String relName, List<Operator.InputPort> inputPorts, Operator operator, Operator.OutputPort outPort, Class clazz)
  {
    this.inputPorts = inputPorts;
    this.operator = operator;
    this.outPort = outPort;
    this.clazz = clazz;
    this.relName = relName;
    this.outRelDataType = null;
  }

  public Class getClazz()
  {
    return clazz;
  }

  public void setClazz(Class clazz)
  {
    this.clazz = clazz;
  }

  public List<Operator.InputPort> getInputPorts()
  {
    return inputPorts;
  }

  public void setInputPorts(List<Operator.InputPort> inputPorts)
  {
    this.inputPorts = inputPorts;
  }

  public String getRelName()
  {
    return relName;
  }

  public void setRelName(String relName)
  {
    this.relName = relName;
  }

  public Operator getOperator()
  {
    return operator;
  }

  public void setOperator(Operator operator)
  {
    this.operator = operator;
  }

  public Operator.OutputPort getOutPort()
  {
    return outPort;
  }

  public void setOutPort(Operator.OutputPort outPort)
  {
    this.outPort = outPort;
  }

  public RelDataType getOutRelDataType()
  {
    return outRelDataType;
  }

  public void setOutRelDataType(RelDataType outRelDataType)
  {
    this.outRelDataType = outRelDataType;
  }
}
