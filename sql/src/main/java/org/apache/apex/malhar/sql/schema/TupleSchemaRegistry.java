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
package org.apache.apex.malhar.sql.schema;

import java.io.File;
import java.io.IOException;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import org.codehaus.jettison.json.JSONException;

import org.apache.apex.malhar.lib.utils.ClassLoaderUtils;
import org.apache.apex.malhar.sql.codegen.BeanClassGenerator;
import org.apache.apex.malhar.sql.operators.OperatorUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import org.apache.commons.lang.ClassUtils;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

@InterfaceStability.Evolving
/**
 * @since 3.6.0
 */
public class TupleSchemaRegistry
{
  public static final String FQCN_PACKAGE = "org.apache.apex.generated.schema.";
  private Map<String, Schema> schemas = new HashMap<>();

  public Schema createNewSchema(String name)
  {
    if (schemas.containsKey(name)) {
      return schemas.get(name);
    }

    Schema schema = new Schema();
    schema.name = name;
    schemas.put(name, schema);

    return schema;
  }

  public Schema getSchemaDefinition(String name)
  {
    return schemas.get(name);
  }

  public String generateCommonJar() throws IOException
  {
    File file = File.createTempFile("schemaSQL", ".jar");

    FileSystem fs = FileSystem.newInstance(file.toURI(), new Configuration());
    FSDataOutputStream out = fs.create(new Path(file.getAbsolutePath()));
    JarOutputStream jout = new JarOutputStream(out);

    for (Schema schema : schemas.values()) {
      jout.putNextEntry(new ZipEntry(schema.fqcn.replace(".", "/") + ".class"));
      jout.write(schema.beanClassBytes);
      jout.closeEntry();
    }

    jout.close();
    out.close();

    return file.getAbsolutePath();
  }

  public static Class getSchemaForRelDataType(TupleSchemaRegistry registry, String schemaName, RelDataType rowType)
  {
    if (rowType.isStruct()) {
      TupleSchemaRegistry.Schema newSchema = registry.createNewSchema(schemaName);
      for (RelDataTypeField field : rowType.getFieldList()) {
        RelDataType type = field.getType();
        newSchema.addField(OperatorUtils.getValidFieldName(field), convertPrimitiveToSqlType(type));
      }
      try {
        newSchema.generateBean();
      } catch (IOException | JSONException e) {
        throw new RuntimeException("Failed to generate schema", e);
      }
      return newSchema.beanClass;
    } else {
      throw new UnsupportedOperationException("Non-struct row type is not implemented.");
    }
  }

  private static Class convertPrimitiveToSqlType(RelDataType type)
  {
    /* I hope that following this method instead of calling value.value() is better
    because we can catch any type mismatches. */
    switch (type.getSqlTypeName()) {
      case BOOLEAN:
        return Boolean.class;
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return Integer.class;
      case BIGINT:
        return Long.class;
      case REAL:
        return Float.class;
      case FLOAT:
      case DOUBLE:
        return Double.class;
      case DATE:
      case TIME:
      case TIMESTAMP:
        return Date.class;
      case CHAR:
      case VARCHAR:
        return String.class;
      case BINARY:
      case VARBINARY:
        return Byte.class;
      case ANY:
      case SYMBOL:
        return Object.class;
      default:
        throw new RuntimeException(String.format("Unsupported type %s", type.getSqlTypeName()));
    }
  }

  public enum Type
  {
    BOOLEAN(Boolean.class), SHORT(Short.class), INTEGER(Integer.class), LONG(Long.class),
    FLOAT(Float.class), DOUBLE(Double.class), STRING(String.class), OBJECT(Object.class),
    DATE(Date.class), TIME(Time.class);

    private Class javaType;

    Type(Class javaType)
    {
      this.javaType = javaType;
    }

    public static Type getFromJavaType(Class type)
    {
      for (Type supportType : Type.values()) {
        if (supportType.getJavaType() == ClassUtils.primitiveToWrapper(type)) {
          return supportType;
        }
      }

      return OBJECT;
    }

    public Class getJavaType()
    {
      return javaType;
    }
  }

  public static class Schema
  {
    public String name;
    public String fqcn;
    public List<SQLFieldInfo> fieldList = new ArrayList<>();
    public Class beanClass;
    public byte[] beanClassBytes;

    public Schema addField(String fieldName, Class fieldType)
    {
      fieldList.add(new SQLFieldInfo(fieldName, Type.getFromJavaType(fieldType)));
      return this;
    }

    public Schema generateBean() throws IOException, JSONException
    {
      // Generate
      this.fqcn = FQCN_PACKAGE + name;

      // Use Bean Class generator to generate the class
      this.beanClassBytes = BeanClassGenerator.createAndWriteBeanClass(this.fqcn, fieldList);
      this.beanClass = ClassLoaderUtils.readBeanClass(fqcn, beanClassBytes);

      return this;
    }
  }

  public static class SQLFieldInfo
  {
    String columnName;
    Type type;

    public SQLFieldInfo(String columnName, Type type)
    {
      this.columnName = columnName;
      this.type = type;
    }

    public String getColumnName()
    {
      return columnName;
    }

    public Type getType()
    {
      return type;
    }
  }

}
