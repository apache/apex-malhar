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
import java.util.List;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

/**
 * A simple error listener that is plugged into the Kudu expression parser
 *
 * @since 3.8.0
 */
public class KuduSQLExpressionErrorListener extends BaseErrorListener
{
  private boolean syntaxError = false;

  private List<String> listOfErrorMessages = new ArrayList<>();

  @Override
  public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine,
      String msg, RecognitionException e)
  {
    super.syntaxError(recognizer, offendingSymbol, line, charPositionInLine, msg, e);
    syntaxError = true;
    listOfErrorMessages.add(msg);
  }

  public boolean isSyntaxError()
  {
    return syntaxError;
  }

  public void setSyntaxError(boolean syntaxError)
  {
    this.syntaxError = syntaxError;
  }

  public List<String> getListOfErrorMessages()
  {
    return listOfErrorMessages;
  }

  public void setListOfErrorMessages(List<String> listOfErrorMessages)
  {
    this.listOfErrorMessages = listOfErrorMessages;
  }
}
