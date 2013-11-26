package com.datatorrent.lib.database;

/**
 * <br>API of a DB Connection.</br>
 *
 */
public interface DBConnector
{
  public void setupDbConnection();
  public void teardownDbConnection();
}
