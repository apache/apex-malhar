/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import java.util.ArrayList;
import javax.validation.constraints.NotNull;

/**
 * <p>
 * CassandraOutputOperator class.</p>
 * A Generic implementation of AbstractCassandraTransactionableOutputOperator which takes in any POJO.
 * @since 1.0.3
 */
public class CassandraOutputOperator extends AbstractCassandraTransactionableOutputOperator<Object>
{

  private int id = 0;

  protected transient ArrayList<String> identifiers;

  public ArrayList<String> getIdentifiers()
  {
    return identifiers;
  }

  public void setIdentifiers(ArrayList<String> identifiers)
  {
    this.identifiers = identifiers;
  }

  @NotNull
  private String tablename;
  private String keyspace;

  /*
   * The Cassandra keyspace is a namespace that defines how data is replicated on nodes.
   * Typically, a cluster has one keyspace per application. Replication is controlled on a per-keyspace basis, so data that has different replication requirements typically resides in different keyspaces.
   * Keyspaces are not designed to be used as a significant map layer within the data model. Keyspaces are designed to control data replication for a set of tables.
   */
  public String getKeyspace()
  {
    return keyspace;
  }

  public void setKeyspace(String keyspace)
  {
    this.keyspace = keyspace;
  }

  /*
   * Tablename in cassandra.
   */
  public String getTablename()
  {
    return tablename;
  }

  public void setTablename(String tablename)
  {
    this.tablename = tablename;
  }

  public enum FIELD_TYPE
  {
    BOOLEAN, INTEGER, FLOAT, DOUBLE, LONG, STRING, UUID, TIMESTAMP
  };




  @Override
  protected PreparedStatement getUpdateCommand()
  {
    StringBuilder queryfields = new StringBuilder();
    StringBuilder values = new StringBuilder();
    for (int i = 0; i < identifiers.size(); i++) {
      queryfields.append(identifiers.get(i));
      values.append("?");
      if(i<=identifiers.size()-1){
        queryfields.append(",");
        values.append(",");
      }

    }
    String statement = "Insert into " + keyspace + "." + tablename + "("+ queryfields+ ") values ("+values+");";
    return store.getSession().prepare(statement);
  }

  @Override
  protected Statement setStatementParameters(PreparedStatement updateCommand, Object tuple) throws DriverException
  {
    BoundStatement boundStmnt = new BoundStatement(updateCommand);
    return boundStmnt.bind(id++, tuple);
  }

}
