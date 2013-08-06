package com.datatorrent.lib.streamquery.condition;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.validation.constraints.NotNull;



public class LikeCondition extends Condition
{
  @NotNull
  private String column;
  @NotNull
  private Pattern pattern;
  
  public LikeCondition(String column, String pattern) {
    setColumn(column);
    setPattern(pattern);
  }
  
  @Override
  public boolean isValidRow(Map<String, Object> row)
  {
    if (!row.containsKey(column)) return false;
    Matcher match = pattern.matcher((CharSequence) row.get(column));
    if (!match.find()) return false;
    return true;
  }

  @Override
  public boolean isValidJoin(Map<String, Object> row1, Map<String, Object> row2)
  {
    // TODO Auto-generated method stub
    return false;
  }

  public String getColumn()
  {
    return column;
  }

  public void setColumn(String column)
  {
    this.column = column;
  }

  public void setPattern(String pattern)
  {
    this.pattern = Pattern.compile(pattern);
  }

}
