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

package org.apache.apex.malhar.hive;

import java.util.ArrayList;
import java.util.Arrays;

import javax.annotation.Nonnull;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.hive.FSPojoToHiveOperator.FIELD_TYPE;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.converters.AbstractConverter;
import org.apache.commons.beanutils.converters.ArrayConverter;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;

/**
 * HiveOutputModule provides abstraction for the operators needed for writing
 * tuples to hive. This module will be expanded to FSPojoToHiveOperator and
 * HiveOperator in physical plan.
 *
 * @since 3.5.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class HiveOutputModule implements Module
{

  /**
   * The path of the directory to where files are written.
   */
  @NotNull
  private String filePath;

  /**
   * Names of the columns in hive table (excluding partitioning columns).
   */
  private String[] hiveColumns;

  /**
   * Data types of the columns in hive table (excluding partitioning columns).
   * This sequence should match to the fields in hiveColumnDataTypes
   */
  private FIELD_TYPE[] hiveColumnDataTypes;

  /**
   * Expressions for the hive columns (excluding partitioning columns). This
   * sequence should match to the fields in hiveColumnDataTypes
   */
  private String[] expressionsForHiveColumns;

  /**
   * Names of the columns on which hive data should be partitioned
   */
  private String[] hivePartitionColumns;

  /**
   * Data types of the columns on which hive data should be partitioned. This
   * sequence should match to the fields in hivePartitionColumns
   */
  private FIELD_TYPE[] hivePartitionColumnDataTypes;

  /**
   * Expressions for the hive partition columns. This sequence should match to
   * the fields in hivePartitionColumns
   */
  private String[] expressionsForHivePartitionColumns;

  /**
   * The maximum length in bytes of a rolling file. Default value is 128MB.
   */
  @Min(1)
  protected Long maxLength = 134217728L;

  /**
   * Connection URL for connecting to hive.
   */
  @NotNull
  private String databaseUrl;

  /**
   * Driver for connecting to hive.
   */
  @NotNull
  private String databaseDriver;

  /**
   * Username for connecting to hive
   */
  private String userName;

  /**
   * Password for connecting to hive
   */
  private String password;

  /**
   * Table name for writing data into hive
   */
  @Nonnull
  protected String tablename;

  /**
   * Input port for files metadata.
   */
  public final transient ProxyInputPort<Object> input = new ProxyInputPort<Object>();

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FSPojoToHiveOperator fsRolling = dag.addOperator("fsRolling", new FSPojoToHiveOperator());
    HiveOperator hiveOperator = dag.addOperator("HiveOperator", new HiveOperator());

    input.set(fsRolling.input);
    dag.addStream("toHive", fsRolling.outputPort, hiveOperator.input);
    fsRolling.setFilePath(filePath);
    fsRolling.setHiveColumns(new ArrayList<String>(Arrays.asList(hiveColumns)));
    fsRolling.setHiveColumnDataTypes(new ArrayList<FIELD_TYPE>(Arrays.asList(hiveColumnDataTypes)));
    fsRolling.setExpressionsForHiveColumns(new ArrayList<String>(Arrays.asList(expressionsForHiveColumns)));

    fsRolling.setHivePartitionColumns(new ArrayList<String>(Arrays.asList(hivePartitionColumns)));
    fsRolling.setHivePartitionColumnDataTypes(new ArrayList<FIELD_TYPE>(Arrays.asList(hivePartitionColumnDataTypes)));
    fsRolling.setExpressionsForHivePartitionColumns(
        new ArrayList<String>(Arrays.asList(expressionsForHivePartitionColumns)));

    fsRolling.setMaxLength(maxLength);
    fsRolling.setAlwaysWriteToTmp(true);
    fsRolling.setRotationWindows(0);

    hiveOperator.setHivePartitionColumns(new ArrayList<String>(Arrays.asList(hivePartitionColumns)));
    HiveStore hiveStore = hiveOperator.getStore();
    hiveStore.setFilepath(filePath);
    hiveStore.setDatabaseUrl(databaseUrl);
    hiveStore.setDatabaseDriver(databaseDriver);
    hiveStore.getConnectionProperties().put("user", userName);
    if (password != null) {
      hiveStore.getConnectionProperties().put("password", password);
    }
    hiveOperator.setTablename(tablename);
  }

  /**
   * The path of the directory to where files are written.
   *
   * @return file path
   */
  public String getFilePath()
  {
    return filePath;
  }

  /**
   * The path of the directory to where files are written.
   *
   * @param filePath
   *          file path
   */
  public void setFilePath(String filePath)
  {
    this.filePath = filePath;
  }

  /**
   * Names of the columns in hive table (excluding partitioning columns).
   *
   * @return Hive column names
   */
  public String[] getHiveColumns()
  {
    return hiveColumns;
  }

  /**
   * Names of the columns in hive table (excluding partitioning columns).
   *
   * @param hiveColumns
   *          Hive column names
   */
  public void setHiveColumns(String[] hiveColumns)
  {
    this.hiveColumns = hiveColumns;
  }

  /**
   * Data types of the columns in hive table (excluding partitioning columns).
   * This sequence should match to the fields in hiveColumnDataTypes
   *
   * @return Hive column data types
   */
  public FIELD_TYPE[] getHiveColumnDataTypes()
  {
    return hiveColumnDataTypes;
  }

  /**
   * Data types of the columns in hive table (excluding partitioning columns).
   * This sequence should match to the fields in hiveColumnDataTypes *
   *
   * @param hiveColumnDataTypes
   *          Hive column data types
   */
  public void setHiveColumnDataTypes(FIELD_TYPE[] hiveColumnDataTypes)
  {
    this.hiveColumnDataTypes = hiveColumnDataTypes;
  }

  /**
   * Expressions for the hive columns (excluding partitioning columns). This
   * sequence should match to the fields in hiveColumnDataTypes
   *
   * @return
   */
  public String[] getExpressionsForHiveColumns()
  {
    return expressionsForHiveColumns;
  }

  /**
   * Expressions for the hive columns (excluding partitioning columns). This
   * sequence should match to the fields in hiveColumnDataTypes
   *
   * @param expressionsForHiveColumns
   */
  public void setExpressionsForHiveColumns(String[] expressionsForHiveColumns)
  {
    this.expressionsForHiveColumns = expressionsForHiveColumns;
  }

  /**
   * Names of the columns on which hive data should be partitioned
   *
   * @return hive partition columns
   */
  public String[] getHivePartitionColumns()
  {
    return hivePartitionColumns;
  }

  /**
   * Names of the columns on which hive data should be partitioned
   *
   * @param hivePartitionColumns
   *          Hive partition columns
   */
  public void setHivePartitionColumns(String[] hivePartitionColumns)
  {
    this.hivePartitionColumns = hivePartitionColumns;
  }

  /**
   * Data types of the columns on which hive data should be partitioned. This
   * sequence should match to the fields in hivePartitionColumns
   *
   * @return Hive partition column data types
   */
  public FIELD_TYPE[] getHivePartitionColumnDataTypes()
  {
    return hivePartitionColumnDataTypes;
  }

  /**
   * Data types of the columns on which hive data should be partitioned. This
   * sequence should match to the fields in hivePartitionColumns
   *
   * @param hivePartitionColumnDataTypes
   *          Hive partition column data types
   */
  public void setHivePartitionColumnDataTypes(FIELD_TYPE[] hivePartitionColumnDataTypes)
  {
    this.hivePartitionColumnDataTypes = hivePartitionColumnDataTypes;
  }

  /**
   * Expressions for the hive partition columns. This sequence should match to
   * the fields in hivePartitionColumns
   *
   * @return Expressions for hive partition columns
   */
  public String[] getExpressionsForHivePartitionColumns()
  {
    return expressionsForHivePartitionColumns;
  }

  /**
   * Expressions for the hive partition columns. This sequence should match to
   * the fields in hivePartitionColumns
   *
   * @param expressionsForHivePartitionColumns
   *          Expressions for hive partition columns
   */
  public void setExpressionsForHivePartitionColumns(String[] expressionsForHivePartitionColumns)
  {
    this.expressionsForHivePartitionColumns = expressionsForHivePartitionColumns;
  }

  /**
   * The maximum length in bytes of a rolling file.
   *
   * @return maximum size of file
   */
  public Long getMaxLength()
  {
    return maxLength;
  }

  /**
   * The maximum length in bytes of a rolling file.
   *
   * @param maxLength
   *          maximum size of file
   */
  public void setMaxLength(Long maxLength)
  {
    this.maxLength = maxLength;
  }

  /**
   * Connection URL for connecting to hive.
   *
   * @return database url
   */
  public String getDatabaseUrl()
  {
    return databaseUrl;
  }

  /**
   * Connection URL for connecting to hive.
   *
   * @param databaseUrl
   *          database url
   */
  public void setDatabaseUrl(String databaseUrl)
  {
    this.databaseUrl = databaseUrl;
  }

  /**
   * Driver for connecting to hive.
   *
   * @return database driver
   */
  public String getDatabaseDriver()
  {
    return databaseDriver;
  }

  /**
   * Driver for connecting to hive.
   *
   * @param databaseDriver
   *          database driver
   */
  public void setDatabaseDriver(String databaseDriver)
  {
    this.databaseDriver = databaseDriver;
  }

  /**
   * Username for connecting to hive
   *
   * @return user name
   */
  public String getUserName()
  {
    return userName;
  }

  /**
   * Username for connecting to hive
   *
   * @param username
   *          user name
   */
  public void setUserName(String userName)
  {
    this.userName = userName;
  }

  /**
   * Password for connecting to hive
   *
   * @return password
   */
  public String getPassword()
  {
    return password;
  }

  /**
   * Password for connecting to hive
   *
   * @param password
   *          password
   */
  public void setPassword(String password)
  {
    this.password = password;
  }

  /**
   * Table name for writing data into hive
   *
   * @return table name
   */
  public String getTablename()
  {
    return tablename;
  }

  /**
   * Table name for writing data into hive
   *
   * @param tablename
   *          table name
   */
  public void setTablename(String tablename)
  {
    this.tablename = tablename;
  }

  static {
    //Code for enabling BeanUtils to accept comma separated string to initialize FIELD_TYPE[]
    class FieldTypeConvertor extends AbstractConverter
    {

      @Override
      protected Object convertToType(Class type, Object value) throws Throwable
      {
        if (value instanceof String) {

          return FIELD_TYPE.valueOf((String)value);
        } else {
          throw new IllegalArgumentException("FIELD_TYPE should be specified as String");
        }
      }

      @Override
      protected Class getDefaultType()
      {
        return FIELD_TYPE.class;
      }
    }

    class FieldTypeArrayConvertor extends ArrayConverter
    {

      public FieldTypeArrayConvertor()
      {
        super(FIELD_TYPE[].class, new FieldTypeConvertor());
      }
    }

    ConvertUtils.register(new FieldTypeConvertor(), FIELD_TYPE.class);
    ConvertUtils.register(new FieldTypeConvertor(), FIELD_TYPE.class);
    ConvertUtils.register(new FieldTypeArrayConvertor(), FIELD_TYPE[].class);

    ConvertUtils.lookup(FIELD_TYPE.class).getClass();
  }

}
