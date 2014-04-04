/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.apps.etl;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.script.*;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.google.common.base.Predicate;

import com.datatorrent.lib.script.JavaScriptOperator;

public class JavaScriptEvalPredicate implements Predicate<Map<String, Object>>
{
  @Nonnull
  private String expression;
  @Nonnull
  protected List<String> expressionKeys;
  protected EvaluatorBindings scriptBindings = new EvaluatorBindings();

  protected transient ScriptEngineManager sem;
  protected transient ScriptEngine engine;
  protected transient SimpleScriptContext scriptContext;

  @DefaultSerializer(value = JavaScriptOperator.BindingsSerializer.class)
  protected static class EvaluatorBindings extends SimpleBindings
  {
  }

  public JavaScriptEvalPredicate()
  {
    sem = new ScriptEngineManager();
    engine = sem.getEngineByName("JavaScript");
    scriptContext = new SimpleScriptContext();
  }

  public void setExpression(@Nonnull String expression)
  {
    this.expression = expression;
  }

  public void setExpressionKeys(@Nonnull List<String> expressionKeys)
  {
    this.expressionKeys = expressionKeys;
  }

  public void init()
  {
    scriptContext.setBindings(scriptBindings, ScriptContext.ENGINE_SCOPE);
    engine.setContext(this.scriptContext);
  }

  @Override
  public boolean apply(@Nullable Map<String, Object> input)
  {
    if (input == null) {
      return false;
    }
    for (String expressionKey : expressionKeys) {
      Object value = input.get(expressionKey);
      if (value != null) {
        scriptContext.getBindings(ScriptContext.ENGINE_SCOPE).put(expressionKey, value);
      }
    }
    boolean evalResult;
    try {
      evalResult = (Boolean) engine.eval(expression, scriptContext);
    }
    catch (ScriptException e) {
      throw new RuntimeException(e);
    }
    return evalResult;
  }
}
