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
package org.apache.apex.malhar.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.sql.planner.RelNodeVisitor;
import org.apache.apex.malhar.sql.schema.ApexSQLTable;
import org.apache.apex.malhar.sql.table.Endpoint;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.util.Util;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import com.datatorrent.api.DAG;

/**
 * SQL Execution Environment for SQL integration API of Apex.
 * This exposes calcite functionality in simple way. Most of the APIs are with builder pattern which makes it
 * easier to construct a DAG using this object.
 *
 * Eg.
 * <pre>
 * SQLExecEnvironment.getEnvironment(dag)
 *                   .registerTable("TABLE1", Object of type {@link Endpoint})
 *                   .registerTable("TABLE2", Object of type {@link Endpoint})
 *                   .executeSQL("INSERT INTO TABLE2 SELECT STREAM * FROM TABLE1);
 * </pre>
 *
 * Above code will evaluate SQL statement and convert the resultant Relational Algebra to a sub-DAG.
 *
 * @since 3.6.0
 */
@InterfaceStability.Evolving
public class SQLExecEnvironment
{
  private static final Logger logger = LoggerFactory.getLogger(SQLExecEnvironment.class);

  private final JavaTypeFactoryImpl typeFactory;
  private SchemaPlus schema = Frameworks.createRootSchema(true);

  /**
   * Construct SQL Execution Environment which works on given DAG objec.
   */
  private SQLExecEnvironment()
  {
    this.typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  }

  /**
   * Given SQLExec{@link SQLExecEnvironment} object for given {@link DAG}.
   *
   * @return Returns {@link SQLExecEnvironment} object
   */
  public static SQLExecEnvironment getEnvironment()
  {
    return new SQLExecEnvironment();
  }

  /**
   * Use given model file to initialize {@link SQLExecEnvironment}.
   * The model file contains definitions of endpoints and data formats.
   * Example of file format is like following:
   * <pre>
   *   {
   *     version: '1.0',
   *     defaultSchema: 'APEX',
   *     schemas: [{
   *       name: 'APEX',
   *       tables: [
   *         {
   *            name: 'ORDERS',
   *            type: 'custom',
   *            factory: 'org.apache.apex.malhar.sql.schema.ApexSQLTableFactory',
   *            stream: {
   *              stream: true
   *            },
   *            operand: {
   *              endpoint: 'file',
   *              messageFormat: 'csv',
   *              endpointOperands: {
   *                directory: '/tmp/input'
   *              },
   *              messageFormatOperands: {
   *                schema: '{"separator":",","quoteChar":"\\"","fields":[{"name":"RowTime","type":"Date","constraints":{"format":"dd/MM/yyyy hh:mm:ss"}},{"name":"id","type":"Integer"},{"name":"Product","type":"String"},{"name":"units","type":"Integer"}]}'
   *            }
   *            }
   *         }
   *       ]
   *     }]
   *   }
   * </pre>
   *
   * @param model String content of model file.
   * @return Returns this {@link SQLExecEnvironment}
   */
  public SQLExecEnvironment withModel(String model)
  {
    if (model == null) {
      return this;
    }

    Properties p = new Properties();
    p.put("model", "inline:" + model);
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", p)) {
      CalciteConnection conn = connection.unwrap(CalciteConnection.class);
      this.schema = conn.getRootSchema().getSubSchema(connection.getSchema());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    return this;
  }

  /**
   * Register a table using {@link Endpoint} with this {@link SQLExecEnvironment}
   *
   * @param name Name of the table that needs to be present with SQL Statement.
   * @param endpoint Object of type {@link Endpoint}
   *
   * @return Returns this {@link SQLExecEnvironment}
   */
  public SQLExecEnvironment registerTable(String name, Endpoint endpoint)
  {
    Preconditions.checkNotNull(name, "Table name cannot be null");
    registerTable(name, new ApexSQLTable(schema, name, endpoint));
    return this;
  }

  /**
   * Register a table using {@link Table} with this {@link SQLExecEnvironment}
   *
   * @param name Name of the table that needs to be present with SQL statement
   * @param table Object of type {@link Table}
   *
   * @return Returns this {@link SQLExecEnvironment}
   */
  public SQLExecEnvironment registerTable(String name, Table table)
  {
    Preconditions.checkNotNull(name, "Table name cannot be null");
    schema.add(name, table);
    return this;
  }

  /**
   * Register custom function with this {@link SQLExecEnvironment}
   *
   * @param name Name of the scalar SQL function that needs made available to SQL Statement
   * @param fn Object of type {@link Function}
   *
   * @return Returns this {@link SQLExecEnvironment}
   */
  public SQLExecEnvironment registerFunction(String name, Function fn)
  {
    Preconditions.checkNotNull(name, "Function name cannot be null");
    schema.add(name, fn);
    return this;
  }

  /**
   * Register custom function from given static method with this {@link SQLExecEnvironment}
   *
   * @param name Name of the scalar SQL function that needs make available to SQL Statement
   * @param clazz {@link Class} which contains given static method
   * @param methodName Name of the method from given clazz
   *
   * @return Return this {@link SQLExecEnvironment}
   */
  public SQLExecEnvironment registerFunction(String name, Class clazz, String methodName)
  {
    Preconditions.checkNotNull(name, "Function name cannot be null");
    ScalarFunction scalarFunction = ScalarFunctionImpl.create(clazz, methodName);
    return registerFunction(name, scalarFunction);
  }

  /**
   * This is the main method takes SQL statement as input and contructs a DAG using contructs registered with this
   * {@link SQLExecEnvironment}.
   *
   * @param sql SQL statement that should be converted to a DAG.
   */
  public void executeSQL(DAG dag, String sql)
  {
    FrameworkConfig config = buildFrameWorkConfig();
    Planner planner = Frameworks.getPlanner(config);
    try {
      logger.info("Parsing SQL statement: {}", sql);
      SqlNode parsedTree = planner.parse(sql);
      SqlNode validatedTree = planner.validate(parsedTree);
      RelNode relationalTree = planner.rel(validatedTree).rel;
      logger.info("RelNode relationalTree generate from SQL statement is:\n {}",
          Util.toLinux(RelOptUtil.toString(relationalTree)));
      RelNodeVisitor visitor = new RelNodeVisitor(dag, typeFactory);
      visitor.traverse(relationalTree);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      planner.close();
    }
  }

  /**
   * Method method build a calcite framework configuration for calcite to parse SQL and generate relational tree
   * out of it.
   * @return FrameworkConfig
   */
  private FrameworkConfig buildFrameWorkConfig()
  {
    List<SqlOperatorTable> sqlOperatorTables = new ArrayList<>();
    sqlOperatorTables.add(SqlStdOperatorTable.instance());
    sqlOperatorTables
      .add(new CalciteCatalogReader(CalciteSchema.from(schema), false, Collections.<String>emptyList(), typeFactory));
    return Frameworks.newConfigBuilder().defaultSchema(schema)
      .parserConfig(SqlParser.configBuilder().setLex(Lex.MYSQL).build())
      .operatorTable(new ChainedSqlOperatorTable(sqlOperatorTables)).build();
  }
}
