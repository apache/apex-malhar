package com.datatorrent.lib.database;

/**
 * <br>API of a DB Connection.</br>
 *
 * @since 0.9.1
 */
public interface DBConnector
{
  public void setupDbConnection();
  public void teardownDbConnection();
}
