package com.datatorrent.lib.streamquery.index;

import java.util.Map;

import javax.validation.constraints.NotNull;

public class StringLenIndex  extends ColumnIndex
{
  public StringLenIndex(@NotNull String column, String alias)
  {
    super(column, alias);
  }

  @Override
  public void filter(@NotNull  Map<String, Object> row, @NotNull  Map<String, Object> collect)
  {
    if (!row.containsKey(column)) return;
    if (!(row.get(column) instanceof String)) {
      assert(false);
    }
    
    String name = getColumn();
    if (alias != null) name = alias;
    collect.put(name, ((String)row.get(column)).length());
  }
}
