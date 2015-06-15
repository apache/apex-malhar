/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.cassandra;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Setter;
import com.datatorrent.lib.util.PojoUtils.SetterBoolean;
import com.datatorrent.lib.util.PojoUtils.SetterDouble;
import com.datatorrent.lib.util.PojoUtils.SetterFloat;
import com.datatorrent.lib.util.PojoUtils.SetterInt;
import com.datatorrent.lib.util.PojoUtils.SetterLong;
import java.math.BigDecimal;
import java.util.*;
import javax.validation.constraints.NotNull;

public class CassandraPOJOInputOperator extends AbstractCassandraInputOperator<Object>
{
  @NotNull
  private ArrayList<String> columns;
  private final ArrayList<DataType> columnDataTypes;
  @NotNull
  private ArrayList<String> expressions;
  @NotNull
  private String tablename;
  private transient ArrayList<Object> setters;
  private String retrieveQuery;

  /*
   * Output POJO being input by the user.
   * POJOs will be generated on fly in later implementation.
   */
  private String outputClass;

  public String getOutputClass()
  {
    return outputClass;
  }

  public void setOutputClass(String outputClass)
  {
    this.outputClass = outputClass;
  }

  public String getRetrieveQuery()
  {
    return retrieveQuery;
  }

  public void setRetrieveQuery(String retrieveQuery)
  {
    this.retrieveQuery = retrieveQuery;
  }

  public ArrayList<String> getExpressions()
  {
    return expressions;
  }

  public void setExpressions(ArrayList<String> expressions)
  {
    this.expressions = expressions;
  }


  /*
   * An ArrayList of Columns in the Cassandra Table.
   */
  public ArrayList<String> getColumns()
  {
    return columns;
  }

  public void setColumns(ArrayList<String> columns)
  {
    this.columns = columns;
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

  public CassandraPOJOInputOperator()
  {
    super();
    columnDataTypes = new ArrayList<DataType>();
    setters = new ArrayList<Object>();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object getTuple(Row row)
  {
    Class<?> className = null;
    Object obj = null;

    com.datastax.driver.core.ResultSet rs = store.getSession().execute("select * from " + store.keyspace + "." + tablename);
    final ColumnDefinitions rsMetaData = rs.getColumnDefinitions();

    final int numberOfColumns = rsMetaData.size();
    if (setters.isEmpty()) {
      System.out.println("create setters");
    try {
      className = Class.forName(outputClass);
      obj = className.newInstance();
    }
    catch (ClassNotFoundException ex) {
      throw new RuntimeException(ex);
    }
    catch (InstantiationException ex) {
      throw new RuntimeException(ex);
    }
    catch (IllegalAccessException ex) {
      throw new RuntimeException(ex);
    }
      for (int i = 0; i < numberOfColumns; i++) {
        // get the designated column's data type.
        final DataType type = rsMetaData.getType(i);
        columnDataTypes.add(type);
        Object setter = null;
        final String setterExpr = expressions.get(i);
        switch (type.getName()) {
          case ASCII:
          case TEXT:
          case VARCHAR:
            setter = PojoUtils.createSetter(className, setterExpr, String.class);
            break;
          case BOOLEAN:
            setter = PojoUtils.createSetterBoolean(className, setterExpr);
            break;
          case INT:
            setter = PojoUtils.createSetterInt(className, setterExpr);
            break;
          case BIGINT:
          case COUNTER:
            setter = PojoUtils.createSetterLong(className, setterExpr);
            break;
          case FLOAT:
            setter = PojoUtils.createSetterFloat(className, setterExpr);
            break;
          case DOUBLE:
            setter = PojoUtils.createSetterDouble(className, setterExpr);
            break;
          case DECIMAL:
            setter = PojoUtils.createSetter(className, setterExpr, BigDecimal.class);
            break;
          case SET:
            setter = PojoUtils.createSetter(className, setterExpr, Set.class);
            break;
          case MAP:
            setter = PojoUtils.createSetter(className, setterExpr, Map.class);
            break;
          case LIST:
            setter = PojoUtils.createSetter(className, setterExpr, List.class);
            break;
          case TIMESTAMP:
            setter = PojoUtils.createSetter(className, setterExpr, Date.class);
            break;
          case UUID:
            setter = PojoUtils.createSetter(className, setterExpr, UUID.class);
            break;
          default:
            setter = PojoUtils.createSetter(className, setterExpr, Object.class);
            break;
        }
        setters.add(setter);
      }
    }

    for (int i = 0; i < numberOfColumns; i++) {
      final DataType type = columnDataTypes.get(i);
      switch (type.getName()) {
        case UUID:
          final UUID id = row.getUUID(i);
          System.out.println("id is "+id);
          ((Setter<Object, UUID>)setters.get(i)).set(obj, id);
          break;
        case ASCII:
        case VARCHAR:
        case TEXT:
          final String ascii = row.getString(i);
          System.out.println("ascii is "+ascii);
          ((Setter<Object, String>)setters.get(i)).set(obj, ascii);
          break;
        case BOOLEAN:
          final boolean bool = row.getBool(i);
          ((SetterBoolean)setters.get(i)).set(obj, bool);
          break;
        case INT:
          final int intValue = row.getInt(i);
           System.out.println("age is "+intValue);
          ((SetterInt)setters.get(i)).set(obj, intValue);
          break;
        case BIGINT:
        case COUNTER:
          final long longValue = row.getLong(i);
          ((SetterLong)setters.get(i)).set(obj, longValue);
          break;
        case FLOAT:
          final float floatValue = row.getFloat(i);
          ((SetterFloat)setters.get(i)).set(obj, floatValue);
          break;
        case DOUBLE:
          final double doubleValue = row.getDouble(i);
          ((SetterDouble)setters.get(i)).set(obj, doubleValue);
          break;
        case DECIMAL:
          final BigDecimal decimal = row.getDecimal(i);
          ((Setter<Object, BigDecimal>)setters.get(i)).set(obj, decimal);
          break;
        case SET:
          Set<?> set = row.getSet(i, Object.class);
          ((Setter<Object, Set<?>>)setters.get(i)).set(obj, set);

          break;
        case MAP:
          final Map<?, ?> map = row.getMap(i, Object.class, Object.class);
          ((Setter<Object, Map<?, ?>>)setters.get(i)).set(obj, map);
          break;
        case LIST:
          final List<?> list = row.getList(i, Object.class);
          ((Setter<Object, List<?>>)setters.get(i)).set(obj, list);
          break;
        case TIMESTAMP:
          final Date date = row.getDate(i);
          ((Setter<Object, Date>)setters.get(i)).set(obj, date);
          break;
        default:
          throw new RuntimeException("unsupported data type " + type.getName());
      }
    }
    return obj;
  }

  @Override
  public String queryToRetrieveData()
  {
    return retrieveQuery;
  }

}
