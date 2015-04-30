/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.contrib.memsql;

import com.datatorrent.api.Context.OperatorContext;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
//map<string,object> fieldname value
//pojo
//ordering
public class MemsqlInputOperator extends AbstractMemsqlInputOperator<Object>
{
  private int currentRow = 1;
  private int inputSize = 0;
  private String tablename;
  private String primaryKeyCol;
  private Long currentPrimaryKey;
  private int batchsize;

  public int getBatchsize()
  {
    return batchsize;
  }

  public void setBatchsize(int batchsize)
  {
    this.batchsize = batchsize;
  }

  public String getPrimaryKeyCol()
  {
    return primaryKeyCol;
  }

  public void setPrimaryKeyCol(String primaryKeyCol)
  {
    this.primaryKeyCol = primaryKeyCol;
  }

  public String getTablename()
  {
    return tablename;
  }

  public void setTablename(String tablename)
  {
    this.tablename = tablename;
  }
 // private final String SELECT_COUNT = "select count(*) from " + tablename;


  public MemsqlInputOperator()
  {
  }

  public static class Field
  {
    String name;
    FIELD_TYPE type;

    public String getName()
    {
      return name;
    }

    public void setName(String name)
    {
      this.name = name;
    }

    public FIELD_TYPE getType()
    {
      return type;
    }

    public void setType(String type)
    {
      this.type = FIELD_TYPE.valueOf(type);
    }
  }

  public enum FIELD_TYPE
  {
    BOOLEAN, DOUBLE, INTEGER, FLOAT, LONG, SHORT, CHARACTER, STRING, DATE
  };

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    String minPrimaryKeyStmt = "select min "+ primaryKeyCol +" FROM " +tablename;
    try {
      Statement statement = store.getConnection().createStatement();
      ResultSet resultSet = statement.executeQuery(minPrimaryKeyStmt);
      resultSet.next();
      currentPrimaryKey = (Long)resultSet.getObject(2);
      statement.close();
    }
    catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public Object getTuple(ResultSet result)
  {
    Long valueofprimarykeyid = 0L;
    try{
     valueofprimarykeyid = (Long)result.getObject(primaryKeyCol);
    }
    catch(SQLException ex)
    {
      throw new RuntimeException(ex);
    }
    if (valueofprimarykeyid >= currentPrimaryKey){
      currentPrimaryKey= valueofprimarykeyid + 1 ;
    }
    Object tuple = null;

    try {
      tuple = result.getInt(2);
    }
    catch (SQLException ex) {
      throw new RuntimeException(ex);
    }

    return tuple;
  }

  @Override
  public void emitTuples()
  {
    super.emitTuples();
  }

  @Override
  public String queryToRetrieveData()
  {
    if (currentRow > inputSize) {
      return null;
    }

    StringBuilder sb = new StringBuilder();
    sb.append("select * from ");
    sb.append(tablename);
    sb.append(" where ");
    sb.append(primaryKeyCol);
    sb.append(" >= ");
    sb.append(currentRow);
    sb.append(" and ");
    sb.append("LIMIT");
    sb.append(batchsize);

    return sb.toString();
  }


}
