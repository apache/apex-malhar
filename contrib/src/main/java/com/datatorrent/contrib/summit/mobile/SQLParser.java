/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
 * limitations under the License. See accompanying LICENSE file.
 */
package com.datatorrent.contrib.summit.mobile;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class SQLParser
{

  private static final String INSERT_QUERY_NAME = "insert";
  private static final String DELETE_QUERY_NAME = "delete";

  private static final String INSERT_INTO = "into";
  private static final String INSERT_VALUES = "values";

  private static final String DELETE_FROM = "from";
  private static final String DELETE_WHERE = "where";
  private static final String DELETE_IN = "in";

  public SQLQuery parseSQLQuery(String queryStr) {
    SQLQuery sqlQuery = null;
    queryStr = queryStr.trim();
    queryStr = queryStr.toLowerCase();
    if (queryStr.startsWith(INSERT_QUERY_NAME)) {
      sqlQuery = parseSQLInsert(queryStr);
    } else if (queryStr.startsWith(DELETE_QUERY_NAME)) {
      sqlQuery = parseSQLDelete(queryStr);
    }
    return sqlQuery;
  }

  public SQLInsert parseSQLInsert(String query) {
    SQLInsert sqlInsert = null;
    String entityName = null;
    List<String> names = null;
    List<List<String>> values = null;
    int nextToken = 0;
    List<String> tokens = splitSQL(query);
    for (String token : tokens) {
      if  (token.equals(INSERT_INTO)) {
        nextToken = 1;
      }else if (nextToken == 1) {
        entityName = token;
        nextToken = 2;
      } else if (nextToken == 2) {
        if (!token.equals(INSERT_VALUES)) {
          names = parseSQLList(token);
          nextToken = 0;
        } else {
          nextToken = 3;
        }
      } else if (token.equals(INSERT_VALUES)) {
        nextToken = 3;
      } else if (nextToken == 3) {
        if (!token.equals(",")) {
          List<String> val = parseSQLList(token);
          if (values == null) {
            values = new ArrayList<List<String>>();
          }
          values.add(val);
        }
      }
    }
    if ((entityName != null) && (values != null)) {
      sqlInsert = new SQLInsert();
      sqlInsert.setEntityName(entityName);
      sqlInsert.setNames(names);
      sqlInsert.setValues(values);
    }
    return sqlInsert;
  }

  public SQLDelete parseSQLDelete(String query) {
    SQLDelete sqlDelete = null;
    String entityName = null;
    String name = null;
    List<String> values = null;
    int nextToken = 0;
    List<String> tokens = splitSQL(query);
    for (String token : tokens) {
      if  (token.equals(DELETE_FROM)) {
        nextToken = 1;
      } else if (nextToken == 1) {
        entityName = token;
        nextToken = 0;
      } else if (token.equals(DELETE_WHERE)) {
        nextToken = 2;
      } else if (nextToken == 2) {
          name = token;
          nextToken = 0;
      } else if (token.equals(DELETE_IN)) {
          nextToken = 3;
      } else if (nextToken == 3) {
          values = parseSQLList(token);
      }
    }
    if ((entityName != null) && (((name == null) && (values == null))
             || ((name != null) && (values != null))) ) {
      sqlDelete = new SQLDelete();
      sqlDelete.setEntityName(entityName);
      sqlDelete.setName(name);
      sqlDelete.setValues(values);
    }
    return sqlDelete;
  }

  private List<String> parseSQLList(String s) {
    List<String> tokens = null;
    String[] stokens = s.split("[ \\(\\),]");
    for (int i = 0; i < stokens.length; ++i) {
      if (!stokens[i].trim().isEmpty()) {
        if (tokens == null) {
          tokens = new ArrayList<String>();
        }
        tokens.add(stokens[i]);
      }
    }
    return tokens;
  }

  private List<String> splitSQL(String s) {
    List<String> tokens = new ArrayList<String>();
    int lastIdx = -1;
    int subToken = 0;
    for (int i = 0; i < s.length(); ++i) {
      if (lastIdx != -1) {
        if ((subToken == 0) && (s.charAt(i) == ' ')) {
          String token = s.substring(lastIdx, i);
          tokens.add(token);
          lastIdx = -1;
        } else if ((subToken == 1) && (s.charAt(i) == ')')) {
          String token = s.substring(lastIdx, i+1);
          tokens.add(token);
          subToken = 0;
          lastIdx = -1;
        }
      } else {
        if (s.charAt(i) == '(') {
          subToken = 1;
          lastIdx = i;
        } else if ((s.charAt(i) != ' ') && (lastIdx == -1)) {
          lastIdx = i;
        }
      }
    }
    return tokens;
  }
}
