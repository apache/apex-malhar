package com.datatorrent.lib.streamquery.index;

import java.util.Map;

import javax.validation.constraints.NotNull;

/**
 * <p>Implements Round Double Index class.</p>
 * <p>
 *  @displayName: Round Double Index
 *  @category: streamquery/index
 *  @tag: alias, maths
 *  @since 0.3.4
 */
public class RoundDoubleIndex  extends ColumnIndex
{
  private int rounder;
  public RoundDoubleIndex(@NotNull String column, String alias, int numDecimals)
  {
    super(column, alias);
    rounder = 1;
    if (numDecimals > 0) rounder = (int) Math.pow(10, numDecimals);
  }

  @Override
  public void filter(@NotNull  Map<String, Object> row, @NotNull  Map<String, Object> collect)
  {
    if (!row.containsKey(column)) return;
    double value = (Double) row.get(column);
    value = Math.round(value * rounder)/rounder;
    String name = getColumn();
    if (alias != null) name = alias;
    collect.put(name, value);
  }
}

