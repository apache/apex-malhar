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

import org.antlr.v4.runtime.TokenStream;

import org.apache.apex.malhar.kudu.sqlparser.KuduSQLExpressionParser;

/**
 * A simple wrapper on the top of the auto-generated KuduSQL expression parser.
 *
 * @since 3.8.0
 */
public class KuduSQLParser extends KuduSQLExpressionParser
{
  private KuduSQLExpressionErrorListener kuduSQLExpressionErrorListener;

  public KuduSQLParser(TokenStream input)
  {
    super(input);
    removeErrorListeners();
    kuduSQLExpressionErrorListener = new KuduSQLExpressionErrorListener();
    addErrorListener(kuduSQLExpressionErrorListener);
  }

  public KuduSQLExpressionErrorListener getKuduSQLExpressionErrorListener()
  {
    return kuduSQLExpressionErrorListener;
  }

  public void setKuduSQLExpressionErrorListener(KuduSQLExpressionErrorListener kuduSQLExpressionErrorListener)
  {
    this.kuduSQLExpressionErrorListener = kuduSQLExpressionErrorListener;
  }
}
