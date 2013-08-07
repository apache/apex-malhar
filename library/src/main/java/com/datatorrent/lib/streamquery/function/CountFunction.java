package com.datatorrent.lib.streamquery.function;

import java.util.ArrayList;
import java.util.Map;

import javax.validation.constraints.NotNull;

public class CountFunction extends FunctionIndex
{

  public CountFunction(@NotNull String column, String alias)
  {
    super(column, alias);
  }

  @Override
  public Object compute(ArrayList<Map<String, Object>> rows) throws Exception
  {
    return rows.size();
  }

  @Override
  protected String aggregateName()
  {
    return "COUNT(" + column + ")";
  }

}
