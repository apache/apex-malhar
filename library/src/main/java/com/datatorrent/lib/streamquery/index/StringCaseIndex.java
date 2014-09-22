package com.datatorrent.lib.streamquery.index;

import java.util.Map;

import javax.validation.constraints.NotNull;

/**
 * <p>A derivation of Column Index that implements filter method using case of a string index. </p>
 *  <p>
 *  @displayName: String Case Index
 *  @category: streamquery/index
 *  @tag: alias
 * @since 0.3.4
 */
public class StringCaseIndex extends  ColumnIndex
{
  private boolean toUpperCase = true; 
  public StringCaseIndex(@NotNull String column, String alias, boolean toLowerCase)
  {
    super(column, alias);
    toUpperCase = !toLowerCase;
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
    if (toUpperCase) {
      collect.put(name, ((String)row.get(column)).toUpperCase());
    } else {
      collect.put(name, ((String)row.get(column)).toLowerCase());
    }
  }
}
