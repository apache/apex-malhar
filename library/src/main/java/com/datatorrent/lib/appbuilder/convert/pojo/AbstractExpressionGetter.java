/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.lib.appbuilder.convert.pojo;

import com.datatorrent.common.util.DTThrowable;
import com.google.common.base.Preconditions;
import javax.validation.constraints.NotNull;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IScriptEvaluator;


public class AbstractExpressionGetter
{
  private String fqClassName;
  private String getterString;

  private IScriptEvaluator se;
  private Object getter;

  public AbstractExpressionGetter(String fqClassName,
                        String getterString,
                        Class castClass,
                        Class getterInterface)
  {
    setFQClassName(fqClassName);
    setGetterString(getterString);

    try {
      se = CompilerFactoryFactory.getDefaultCompilerFactory().newScriptEvaluator();
    }
    catch(Exception ex) {
      DTThrowable.rethrow(ex);
    }
  }

  private void initialize(Class castClass, Class getterInterface)
  {
    try {
      //TODO In the future go through method stack and determine return types to avoid creating
      //an object if a primitive is present instead.

      //Corner cases return type was specified to be Boolean via generics
      //Return type is a Boolean object
      //Return type is a boolean primitive
      getter = (GetterBoolean) se.createFastEvaluator("return (" + castClass.getName() +
                                                                        ") (((" + this.getFQClassName() +
                                                                        ")obj)." + this.getGetterString() + ");",
                                                                        GetterBoolean.class,
                                                                        new String[] {ConvertUtils.DEFAULT_POJO_NAME});
    }
    catch(CompileException ex) {
      DTThrowable.rethrow(ex);
    }
  }

  private void setFQClassName(@NotNull String fqClassName)
  {
    this.fqClassName = Preconditions.checkNotNull(fqClassName);
  }

  private void setGetterString(String getterString)
  {
    Preconditions.checkNotNull(getterString);

    if(getterString.startsWith(".")) {
      getterString = getterString.substring(1);
    }

    if(getterString.isEmpty()) {
      throw new IllegalArgumentException("The getter string: " +
                                         getterString +
                                         "\nis invalid.");
    }

    this.getterString = getterString;
  }

  public String getFQClassName()
  {
    return fqClassName;
  }

  public String getGetterString()
  {
    return getterString;
  }

  /**
   * @return the getter
   */
  public Object getGetter()
  {
    return getter;
  }
}
