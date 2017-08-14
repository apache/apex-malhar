/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.db.jdbc;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context.OperatorContext;

/**
 * <p>
 * JdbcPOJOInsertOutputOperator class.</p>
 * An implementation of AbstractJdbcTransactionableOutputOperator which takes in any POJO.
 *
 * @displayName Jdbc Output Operator
 * @category Output
 * @tags database, sql, pojo, jdbc
 * @since 2.1.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class JdbcPOJONonInsertOutputOperator extends AbstractJdbcPOJOOutputOperator
{
  @NotNull
  String sqlStatement;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);

    columnDataTypes = Lists.newArrayList();
    for (JdbcFieldInfo fi : getFieldInfos()) {
      columnFieldGetters.add(new ActiveFieldInfo(fi));
      columnDataTypes.add(fi.getSqlType());
    }
  }

  @Override
  protected String getUpdateCommand()
  {
    return sqlStatement;
  }

  /**
   * Sets the parameterized SQL query for the JDBC update operation.
   * This can be an update, delete or a merge query.
   * Example: "update testTable set id = ? where name = ?"
   * @param sqlStatement the SQL query
   */
  public void setSqlStatement(String sqlStatement)
  {
    this.sqlStatement = sqlStatement;
  }

  private static final Logger LOG = LoggerFactory.getLogger(JdbcPOJONonInsertOutputOperator.class);
}
