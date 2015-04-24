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
package com.datatorrent.lib.pigquery.generate;

import javax.validation.constraints.NotNull;


/**
 * A base implementation of Generate interface that implements single field foreach generate index.&nbsp; 
 * Subclasses should provide the implementation of evaluate method.  
 * <p>
 * <b>Properties : </b> <br>
 * <b>fieldName : </b> Field name or value argument. <br>
 * <b>aliasName : </b> Alias name for output value. <br>
 * @displayName Generate Unary
 * @category Pig Query
 * @tags string, unary
 * @since 0.3.4
 */
@Deprecated
abstract public class UnaryGenerate  implements Generate 
{
  /**
   * Field Name.
   */
  @NotNull
  protected String fieldName;
  
  /**
   * Alias Name.
   */
  protected String aliasName;

  public UnaryGenerate(@NotNull String fieldName, String aliasName)
  {
    this.fieldName = fieldName;
    this.aliasName = aliasName;
  }
  
  /**
   * Get value for fieldName.
   * @return String
   */
  public String getFieldName()
  {
    return fieldName;
  }

  /**
   * Set value for fieldName.
   * @param fieldName set value for fieldName.
   */
  public void setFieldName(String fieldName)
  {
    this.fieldName = fieldName;
  }

  /**
   * Get value for aliasName.
   * @return String
   */
  public String getAliasName()
  {
    return aliasName;
  }

  /**
   * Set value for aliasName.
   * @param aliasName set value for aliasName.
   */
  public void setAliasName(String aliasName)
  {
    this.aliasName = aliasName;
  }
  
  /**
   * @see com.datatorrent.lib.pigquery.generate.Generate#getOutputAliasName()
   */
  @Override
  public String getOutputAliasName()
  {
    if (aliasName != null) return aliasName;
    return fieldName;
  }
}
