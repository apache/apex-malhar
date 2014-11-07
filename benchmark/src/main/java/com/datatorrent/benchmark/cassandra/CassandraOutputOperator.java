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
package com.datatorrent.benchmark.cassandra;


import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datatorrent.contrib.cassandra.AbstractCassandraTransactionableOutputOperatorPS;


/**
 * <p>CassandraOutputOperator class.</p>
 *
 * @since 1.0.3
 */
public class CassandraOutputOperator extends  AbstractCassandraTransactionableOutputOperatorPS<Integer>{

  private int id = 0;

  @Override
  protected PreparedStatement getUpdateCommand() {
    String statement = "Insert into test.cassandra_operator(id, result) values (?,?);";
    return store.getSession().prepare(statement);
  }

  @Override
  protected Statement setStatementParameters(PreparedStatement updateCommand,
      Integer tuple) throws DriverException {
    BoundStatement boundStmnt = new BoundStatement(updateCommand);
    return boundStmnt.bind(id++,tuple);
  }

}
