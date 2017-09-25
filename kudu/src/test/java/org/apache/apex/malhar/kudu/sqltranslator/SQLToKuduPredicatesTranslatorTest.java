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

import java.util.ArrayList;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.apex.malhar.kudu.KuduClientTestCommons;
import org.apache.apex.malhar.kudu.test.KuduClusterAvailabilityTestRule;
import org.apache.apex.malhar.kudu.test.KuduClusterTestContext;
import org.apache.kudu.ColumnSchema;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
public class SQLToKuduPredicatesTranslatorTest extends KuduClientTestCommons
{
  @Rule
  public KuduClusterAvailabilityTestRule kuduClusterAvailabilityTestRule = new KuduClusterAvailabilityTestRule();

  @KuduClusterTestContext(kuduClusterBasedTest = false)
  @Test
  public void testForCompletenessOfSQLExpression() throws Exception
  {
    SQLToKuduPredicatesTranslator translator = new SQLToKuduPredicatesTranslator(
        "select from unittests",new ArrayList<ColumnSchema>(columnDefs.values()));
    translator.parseKuduExpression();
    KuduSQLExpressionErrorListener errorListener = translator.getErrorListener();
    assertEquals(true,errorListener.isSyntaxError());
  }

  @KuduClusterTestContext(kuduClusterBasedTest = false)
  @Test
  public void testForErrorsInColumnAliasesInSQLExpression() throws Exception
  {
    SQLToKuduPredicatesTranslator translator = new SQLToKuduPredicatesTranslator(
        "select intkey as intColumn from unittests",
        new ArrayList<ColumnSchema>(columnDefs.values()));
    KuduSQLExpressionErrorListener errorListener = translator.getErrorListener();
    assertEquals(false,errorListener.isSyntaxError());

    translator = new SQLToKuduPredicatesTranslator(
      "select intkey as intColumn, 'from' as fgh from unittests",
      new ArrayList<ColumnSchema>(columnDefs.values()));
    errorListener = translator.getErrorListener();
    assertEquals(false,errorListener.isSyntaxError());

    translator = new SQLToKuduPredicatesTranslator(
      "select intkey, 'from' from unittests",
      new ArrayList<ColumnSchema>(columnDefs.values()));
    errorListener = translator.getErrorListener();
    assertEquals(false,errorListener.isSyntaxError());

    translator = new SQLToKuduPredicatesTranslator(
      "select intkey, 'from' as fgh from unittests",
      new ArrayList<ColumnSchema>(columnDefs.values()));
    errorListener = translator.getErrorListener();
    assertEquals(false,errorListener.isSyntaxError());
  }

  @KuduClusterTestContext(kuduClusterBasedTest = false)
  @Test
  public void testForErrorsInOptionsInSQLExpression() throws Exception
  {
    SQLToKuduPredicatesTranslator translator = null;
    KuduSQLExpressionErrorListener errorListener = null;

    translator = new SQLToKuduPredicatesTranslator(
      "select intkey as intColumn from unittests using Options",
      new ArrayList<ColumnSchema>(columnDefs.values()));
    errorListener = translator.getErrorListener();
    assertEquals(true,errorListener.isSyntaxError());

    translator = new SQLToKuduPredicatesTranslator(
      "select intkey as intColumn, 'from' as fgh from " +
        " unittests using options READ_SNAPSHOT_TIME = aASDAD",
      new ArrayList<ColumnSchema>(columnDefs.values()));
    errorListener = translator.getErrorListener();
    assertEquals(true,errorListener.isSyntaxError());

    translator = new SQLToKuduPredicatesTranslator(
      "select intkey, 'from' from unittests using options READ_SNAPSHOT_TIME = 2342345",
      new ArrayList<ColumnSchema>(columnDefs.values()));
    errorListener = translator.getErrorListener();
    assertEquals(false,errorListener.isSyntaxError());

    translator = new SQLToKuduPredicatesTranslator(
      "select intkey, xcv as fgh from unittests using options CONTROLTUPLE_MESSAGE = \"done\"",
      new ArrayList<ColumnSchema>(columnDefs.values()));
    errorListener = translator.getErrorListener();
    assertEquals(false,errorListener.isSyntaxError());
  }

  @KuduClusterTestContext(kuduClusterBasedTest = false)
  @Test
  public void testForSelectStarInSQLExpression() throws Exception
  {
    SQLToKuduPredicatesTranslator translator = new SQLToKuduPredicatesTranslator(
        " select * from unittests",
        new ArrayList<ColumnSchema>(columnDefs.values()));
    assertEquals(true,translator.getKuduSQLParseTreeListener().isSelectStarExpressionEnabled());

    translator = new SQLToKuduPredicatesTranslator(
      "select intkey as intColumn from unittests ",
      new ArrayList<ColumnSchema>(columnDefs.values()));
    assertEquals(false,translator.getKuduSQLParseTreeListener().isSelectStarExpressionEnabled());

  }

  @KuduClusterTestContext(kuduClusterBasedTest = false)
  @Test
  public void testForColumnNameExtractionInSQLExpression() throws Exception
  {
    SQLToKuduPredicatesTranslator translator = new SQLToKuduPredicatesTranslator(
        " select introwkey as intColumn, '      from' as flColumn, stringCol from unittests",
        new ArrayList<ColumnSchema>(columnDefs.values()));
    assertEquals(1, translator.getKuduSQLParseTreeListener().getListOfColumnsUsed().size());
    assertEquals(9, translator.getKuduSQLParseTreeListener().getAliases().size());
    assertEquals("intColumn", translator.getKuduSQLParseTreeListener().getAliases().get("introwkey"));
  }

  @Test
  @KuduClusterTestContext(kuduClusterBasedTest = false)
  public void testForReadSnapshotTimeExpression() throws Exception
  {
    SQLToKuduPredicatesTranslator translator = new SQLToKuduPredicatesTranslator(
        " select introwkey as intColumn using options read_snapshot_time = 12345",
        new ArrayList<ColumnSchema>(columnDefs.values()));
    assertEquals(12345L, translator.getKuduSQLParseTreeListener().getReadSnapshotTime().longValue());
    SQLToKuduPredicatesTranslator translatorForNoReadSnapshotTime = new SQLToKuduPredicatesTranslator(
        " select introwkey as intColumn",
        new ArrayList<ColumnSchema>(columnDefs.values()));
    assertEquals(null, translatorForNoReadSnapshotTime.getKuduSQLParseTreeListener().getReadSnapshotTime());

  }
}
