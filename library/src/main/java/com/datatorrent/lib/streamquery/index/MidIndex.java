package com.datatorrent.lib.streamquery.index;

import java.util.Map;

import javax.validation.constraints.NotNull;

/**
 * <p>Implements Mid Index class.</p>
 * <p>
 * @displayName: Mid Index
 * @category: streamquery/index
 * @tag: index
 * @since 0.3.4
 */
public class MidIndex extends ColumnIndex
{
  private int start;
  private int length = 0;
  
  public MidIndex(@NotNull String column, String alias, int start)
  {
    super(column, alias);
    assert(start >= 0);
    this.start = start;  
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
    
    int endIndex = start + length;
    if ((length == 0)||(endIndex > ((String)row.get(column)).length())) {
      collect.put(name, row.get(column));
    } else {
      collect.put(name, ((String)row.get(column)).substring(start, endIndex));
    }
  }

  public int getLength()
  {
    return length;
  }

  public void setLength(int length)
  {
    assert(length > 0);
    this.length = length;
  }
}

