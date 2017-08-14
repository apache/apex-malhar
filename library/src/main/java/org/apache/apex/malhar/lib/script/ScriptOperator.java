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
package org.apache.apex.malhar.lib.script;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * A base implementation of a BaseOperator for language script operator.&nbsp; Subclasses should provide the
   implementation of getting the bindings and process method.
 * Interface for language script operator.
 * <p>
 * @displayName Script
 * @category Scripting
 * @tags script operator, map, string
 * @since 0.3.2
 */
public abstract class ScriptOperator extends BaseOperator
{
  /**
   * Input inBindings port that takes in a map of &lt;String, Object&gt.
   */
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<Map<String, Object>> inBindings = new DefaultInputPort<Map<String, Object>>()
  {
    @Override
    public void process(Map<String, Object> tuple)
    {
      ScriptOperator.this.process(tuple);
    }

  };

  /**
   * Output outBindings port that emits a map of &lt;String, Object&gt.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Map<String, Object>> outBindings = new DefaultOutputPort<Map<String, Object>>();

  /**
   * Output result port that emits an object as the result.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Object> result = new DefaultOutputPort<Object>();
  protected boolean isPassThru = true;
  @NotNull
  protected String script;
  protected List<String> setupScripts = new ArrayList<String>();

  /**
   * Operator must be set pass thru, for output results.
   *
   * @param isPassThru
   */
  public void setPassThru(boolean isPassThru)
  {
    this.isPassThru = isPassThru;
  }

  /**
   * Set script code for execution.
   *
   * @param script
   */
  public void setScript(String script)
  {
    this.script = script;
  }

  public void addSetupScript(String script)
  {
    setupScripts.add(script);
  }

  public abstract void process(Map<String, Object> tuple);

  public abstract Map<String, Object> getBindings();
}
