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
package com.datatorrent.lib.streamquery.condition;

import java.util.Map;

import javax.validation.constraints.NotNull;

/**
 * Class for logical AND/OR select expression. <br>
 * Class provides logical OR or AND function specified in parameters. User can implement
 * complex and/or expression by chaining operator itself.
 * <br>
 * <b> Properties : </b> <br>
 * <b> leftCondition : </b> Left validate row condition . <br>
 * <b> rightCondition : </b> Right validate row condition. <br>
 * <b> logicalOr : </b> OR/AND logical metric flag. <br>
 * <br>
 *
 * @since 0.3.4
 */
public class CompoundCondition extends Condition
{
  /**
   * Left validate row condition .
   */
  @NotNull
  private Condition leftCondition;
  
  /**
   * Right validate row condition .
   */
  @NotNull
  private Condition rightCondition;
  
  /**
   * AND/OR metric flag.
   */
  private boolean logicalOr = true;
  
  /**
   * Constructor for logical or metric.
   * @param leftCondition  Left validate row condition, must be non null. <br>
   * @param rightCondition  Right validate row condition, must be non null. <br>
   */
  public CompoundCondition(Condition leftCondition, Condition rightCondition) {
    this.leftCondition = leftCondition;
    this.rightCondition = rightCondition;
  }
  
  /**
   * Constructor for logical and metric if logical and parameter is true.
   * <br>
   * @param leftCondition  Left validate row condition, must be non null. <br>
   * @param rightCondition  Right validate row condition, must be non null. <br>
   * @param isLogicalAnd  Logical AND if true.
   */
  public CompoundCondition(Condition leftCondition, Condition rightCondition, boolean isLogicalAnd) {
    this.leftCondition = leftCondition;
    this.rightCondition = rightCondition;
    logicalOr = !isLogicalAnd;
  }
  
  @Override
  public boolean isValidRow(Map<String, Object> row)
  {
    if (logicalOr) {
       return leftCondition.isValidRow(row) || rightCondition.isValidRow(row);
    } else {
      return leftCondition.isValidRow(row) && rightCondition.isValidRow(row);
    }
  }

  @Override
  public boolean isValidJoin(Map<String, Object> row1, Map<String, Object> row2)
  {
    // TODO Auto-generated method stub
    return false;
  }

  public Condition getLeftCondition()
  {
    return leftCondition;
  }

  public void setLeftCondition(Condition leftCondition)
  {
    this.leftCondition = leftCondition;
  }

  public Condition getRightCondition()
  {
    return rightCondition;
  }

  public void setRightCondition(Condition rightCondition)
  {
    this.rightCondition = rightCondition;
  }

  public void setLogicalAnd() {
    this.logicalOr = false;
  }
}
