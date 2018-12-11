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
 * <p>A base implementation of Generate interface.&nbsp; Subclasses should provide the 
   implementation of evaluate method. </p>
 *
 * @displayName Binary Generate
 * @category Pig Query
 * @tags binary, string
 * @since 0.3.4
 */
abstract public class BinaryGenerate implements Generate
{
  /**
   * Left argument field name.
   */
  @NotNull
  protected String leftField;

  /**
   * Right argument field name.
   */
  @NotNull
  protected String rightField;

  /**
   * Alias name.
   */
  protected String aliasName;

  /**
   * Operation name.
   */
  protected  String opName;

  public BinaryGenerate(@NotNull String leftField, @NotNull String rightField, String aliasName, String opName) {
    this.leftField = leftField;
    this.rightField = rightField;
    this.aliasName = aliasName;
    this.opName = opName;
  }

  /**
   * Get value for leftField.
   * @return String
   */
  public String getLeftField()
  {
    return leftField;
  }

  /**
   * Set value for leftField.
   * @param leftField set value for leftField.
   */
  public void setLeftField(String leftField)
  {
    this.leftField = leftField;
  }

  /**
   * Get value for rightField.
   * @return String
   */
  public String getRightField()
  {
    return rightField;
  }

  /**
   * Set value for rightField.
   * @param rightField set value for rightField.
   */
  public void setRightField(String rightField)
  {
    this.rightField = rightField;
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
    return new StringBuilder(opName).append("(").append(leftField).append(",").append(rightField).append(")").toString();
  }

  /**
   * Get value for opName.
   * @return String
   */
  public String getOpName()
  {
    return opName;
  }

  /**
   * Set value for opName.
   * @param opName set value for opName.
   */
  public void setOpName(String opName)
  {
    this.opName = opName;
  }
}
