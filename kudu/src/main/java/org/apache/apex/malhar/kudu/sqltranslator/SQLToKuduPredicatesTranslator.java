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
package org.apache.apex.malhar.kudu.sqltranslator;

import java.util.List;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.kudu.sqlparser.KuduSQLExpressionLexer;
import org.apache.kudu.ColumnSchema;

import com.google.common.base.Preconditions;

/**
 * A class responsible for parsing an SQL expression and return parsed Kudu Client equivalent objects.
 * This class is not Thread safe.
 *
 * @since 3.8.0
 */
public class SQLToKuduPredicatesTranslator
{
  private static final transient Logger LOG = LoggerFactory.getLogger(SQLToKuduPredicatesTranslator.class);

  private String sqlExpresssion = null;

  private KuduSQLExpressionErrorListener errorListener = null;

  private KuduSQLParser parser = null;

  private KuduSQLParseTreeListener kuduSQLParseTreeListener = null;

  private List<ColumnSchema> allColumnsForThisTable = null;

  public SQLToKuduPredicatesTranslator(String sqlExpresssionForParsing, List<ColumnSchema> tableColumns)
    throws Exception
  {
    Preconditions.checkNotNull(tableColumns,"Kudu table cannot have null or empty columns");
    Preconditions.checkNotNull(sqlExpresssionForParsing,"Kudu SQL expression cannot be null");
    sqlExpresssion = sqlExpresssionForParsing;
    allColumnsForThisTable = tableColumns;
    parseKuduExpression();
  }

  public void parseKuduExpression() throws Exception
  {
    KuduSQLExpressionLexer lexer = new KuduSQLExpressionLexer(CharStreams.fromString(sqlExpresssion));
    CommonTokenStream tokens = new CommonTokenStream( lexer );
    parser = new KuduSQLParser( tokens );
    errorListener = parser.getKuduSQLExpressionErrorListener();
    ParseTree parserTree = parser.kudusqlexpression();
    if (!errorListener.isSyntaxError()) {
      ParseTreeWalker parseTreeWalker = new ParseTreeWalker();
      kuduSQLParseTreeListener = new KuduSQLParseTreeListener();
      kuduSQLParseTreeListener.setColumnSchemaList(allColumnsForThisTable);
      try {
        parseTreeWalker.walk(kuduSQLParseTreeListener, parserTree);
      } catch (Exception ex) {
        LOG.error(" The supplied SQL expression could not be parsed because " + ex.getMessage(),ex);
        errorListener.setSyntaxError(true);
      }
    } else {
      LOG.error(" Syntax error present in the Kudu SQL expression. Hence not processing");
      List<String> allRegisteredSyntaxErrors = errorListener.getListOfErrorMessages();
      for (String syntaxErrorMessage : allRegisteredSyntaxErrors) {
        LOG.error(" Error : " + syntaxErrorMessage  + " in SQL expression \"" + sqlExpresssion + " \"");
      }
    }
  }


  public KuduSQLExpressionErrorListener getErrorListener()
  {
    return errorListener;
  }

  public void setErrorListener(KuduSQLExpressionErrorListener errorListener)
  {
    this.errorListener = errorListener;
  }

  public String getSqlExpresssion()
  {
    return sqlExpresssion;
  }

  public void setSqlExpresssion(String sqlExpresssion)
  {
    this.sqlExpresssion = sqlExpresssion;
  }

  public KuduSQLParser getParser()
  {
    return parser;
  }

  public void setParser(KuduSQLParser parser)
  {
    this.parser = parser;
  }

  public KuduSQLParseTreeListener getKuduSQLParseTreeListener()
  {
    return kuduSQLParseTreeListener;
  }

  public void setKuduSQLParseTreeListener(KuduSQLParseTreeListener kuduSQLParseTreeListener)
  {
    this.kuduSQLParseTreeListener = kuduSQLParseTreeListener;
  }
}
