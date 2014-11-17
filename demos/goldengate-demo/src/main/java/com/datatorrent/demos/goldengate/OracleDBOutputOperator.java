/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.contrib.goldengate.lib;

import com.datatorrent.lib.db.jdbc.AbstractJdbcNonTransactionableOutputOperator;
import com.datatorrent.lib.db.jdbc.JdbcStore;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class OracleDBOutputOperator extends AbstractJdbcNonTransactionableOutputOperator<Employee, JdbcStore>
{
  private static final String INSERT = "INSERT INTO PROCESSEDEMPLOYEE" +
                                        " (EID, NAME, DEPARTMENT)" +
                                        " values (?, ?, ?)";
  public OracleDBOutputOperator()
  {
    store = new JdbcStore();
  }

  public JdbcStore getStore()
  {
    return store;
  }

  public void setStore(JdbcStore store)
  {
    this.store = store;
  }

  @Override
  protected String getUpdateCommand()
  {
    return INSERT;
  }

  @Override
  protected void setStatementParameters(PreparedStatement statement,
                                        Employee tuple) throws SQLException
  {
    statement.setInt(1, tuple.eid);
    statement.setString(2, tuple.ename);
    statement.setInt(3, tuple.did);
  }
}
