package com.datatorrent.lib.streamquery.condition;

import java.util.Map;

import javax.validation.constraints.NotNull;

public class CompoundCondition extends Condition
{
  @NotNull
  private Condition leftCondition;
  @NotNull
  private Condition rightCondition;
  private boolean logicalOr = true;
  
  public CompoundCondition(Condition leftCondition, Condition rightCondition) {
    this.leftCondition = leftCondition;
    this.rightCondition = rightCondition;
  }
  
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
