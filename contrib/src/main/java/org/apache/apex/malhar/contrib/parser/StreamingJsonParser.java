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
package org.apache.apex.malhar.contrib.parser;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.StringTokenizer;

import org.elasticsearch.common.primitives.Ints;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.parser.Parser;
import org.apache.apex.malhar.lib.util.FieldInfo;
import org.apache.apex.malhar.lib.util.FieldInfo.SupportType;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.util.PojoUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * Operator that parses a JSON string tuple and emits a POJO on the output port
 * and tuples that could not be parsed on error port.Upstream operator needs to
 * ensure that a full JSON record is emitted.<br>
 * <b>Properties</b><br>
 * <b>pojoClass</b>:POJO class <br>
 * <b>(optional)fieldMappingString</b>String of format
 * fieldNameInJson:fieldNameInPOJO:DataType<br>
 * <b>Ports</b> <br>
 * <b>in</b>:input tuple as a String. Each tuple represents a json string<br>
 * <b>out</b>:tuples that are validated as per the user defined POJO are emitted
 * as POJO on this port<br>
 * <b>err</b>:tuples that could not be parsed are emitted on this port as
 * KeyValPair<String,String><br>
 * Key being the tuple and Val being the reason
 *
 * @displayName SimpleStreamingJsonParser
 * @category Parsers
 * @tags json pojo parser streaming
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public class StreamingJsonParser extends Parser<byte[], KeyValPair<String, String>>
{
  private transient JSONParser jsonParser;
  private transient String fieldMappingString;
  private transient List<FieldInfo> fieldInfos;
  private transient List<ActiveFieldInfo> columnFieldSetters;
  protected JsonKeyFinder finder;
  private static final String FIELD_SEPARATOR = ":";
  private static final String RECORD_SEPARATOR = ",";
  private transient ArrayList<String> columnFields;
  private transient Class<?> pojoClass;

  /**
   * @return POJO class
   */
  private Class<?> getPojoClass()
  {
    return pojoClass;
  }

  /**
   * Sets the POJO class
   */
  public void setPojoClass(Class<?> pojoClass)
  {
    this.pojoClass = pojoClass;
  }

  /**
   * Returns a string representing mapping from generic record to POJO fields
   */
  public String getFieldMappingString()
  {
    return fieldMappingString;
  }

  /**
   * Comma separated list mapping a field in JSON schema to POJO field eg :
   * fieldNameInPOJO:fieldNameInJSON:DataType
   */
  public void setFieldMappingString(String pojoFieldsToJsonMapping)
  {
    this.fieldMappingString = pojoFieldsToJsonMapping;
  }

  public StreamingJsonParser()
  {

  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
  }

  @Override
  public void processTuple(byte[] tuple)
  {
    incomingTuplesCount++;
    Object obj = convert(tuple);
    if (obj != null) {
      output.emit(obj);
      emittedObjectCount++;
    }
  }

  /**
   * Parse an incoming tuple & return a POJO object
   */
  @Override
  public Object convert(byte[] tuple)
  {
    String str;
    if (tuple == null) {
      if (err.isConnected()) {
        err.emit(new KeyValPair<String, String>(null, "null tuple"));
      }
      errorTupleCount++;
      return null;
    }

    try {
      str = new String(tuple, "UTF-8");
    } catch (UnsupportedEncodingException e1) {
      if (err.isConnected()) {
        err.emit(new KeyValPair<String, String>(tuple.toString(), "Encoding not supported"));
      }
      errorTupleCount++;
      LOG.error("Encoding not supported", e1);
      throw new RuntimeException(e1);
    }

    try {
      finder.setKeyCount(0);
      finder.getKeyValMap().clear();
      while (!finder.isEnd()) {
        jsonParser.parse(str, finder, true);
        //stop parsing when the required keyCount is reached
        if (finder.getKeyCount() == columnFields.size()) {
          break;
        }
      }
      jsonParser.reset();
      return setPojoFields(finder.getKeyValMap());
    } catch (ParseException | IllegalAccessException | InstantiationException e) {
      if (err.isConnected()) {
        err.emit(new KeyValPair<String, String>(str, e.getMessage()));
      }
      errorTupleCount++;
      LOG.error("Exception in parsing the record", e);
      throw new RuntimeException(e);
    }

  }

  /**
   * Creates a map representing fieldName in POJO:field in JSON:Data type
   *
   * @return List of FieldInfo
   */
  private List<FieldInfo> createFieldInfoMap(String str)
  {
    fieldInfos = new ArrayList<FieldInfo>();
    StringTokenizer strtok = new StringTokenizer(str, RECORD_SEPARATOR);

    while (strtok.hasMoreTokens()) {
      String[] token = strtok.nextToken().split(FIELD_SEPARATOR);
      try {
        fieldInfos.add(new FieldInfo(token[0], token[1], SupportType.valueOf(token[2])));
      } catch (Exception e) {
        LOG.error("Invalid support type", e);
      }
    }
    return fieldInfos;
  }

  @Override
  public KeyValPair<String, String> processErrorTuple(byte[] input)
  {
    throw new UnsupportedOperationException("Not supported");
  }

  /**
   * Class that maps fieldInfo to its getters or setters
   */
  protected static class ActiveFieldInfo
  {
    final FieldInfo fieldInfo;
    Object setterOrGetter;

    ActiveFieldInfo(FieldInfo fieldInfo)
    {
      this.fieldInfo = fieldInfo;
    }

  }

  /**
   * A list of {@link FieldInfo}s where each item maps a column name to a pojo
   * field name.
   */
  private List<FieldInfo> getFieldInfos()
  {
    return fieldInfos;
  }

  /**
   * Sets the {@link FieldInfo}s. A {@link FieldInfo} maps a store column to a
   * pojo field name.<br/>
   * The value from fieldInfo.column is assigned to
   * fieldInfo.pojoFieldExpression.
   *
   * @description $[].columnName name of the Output Field in POJO
   * @description $[].pojoFieldExpression expression to get the respective field
   *              from generic record
   * @useSchema $[].pojoFieldExpression outputPort.fields[].name
   */
  private void setFieldInfos(List<FieldInfo> fieldInfos)
  {
    this.fieldInfos = fieldInfos;
  }

  /**
   * Use reflection to generate field info values if the user has not provided
   * the inputs mapping
   *
   * @return String representing the POJO field to JSON field mapping
   */
  private String generateFieldInfoInputs(Class<?> cls)
  {
    java.lang.reflect.Field[] fields = cls.getDeclaredFields();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < fields.length; i++) {
      java.lang.reflect.Field f = fields[i];
      Class<?> c = ClassUtils.primitiveToWrapper(f.getType());
      sb.append(f.getName()).append(FIELD_SEPARATOR).append(f.getName()).append(FIELD_SEPARATOR)
          .append(c.getSimpleName().toUpperCase()).append(RECORD_SEPARATOR);
    }
    return sb.substring(0, sb.length() - 1);
  }

  /**
   * Adds the Active Fields to the columnFieldSetters {@link ActiveFieldInfo}s
   */
  private void initColumnFieldSetters(List<FieldInfo> fieldInfos)
  {
    for (FieldInfo fi : fieldInfos) {
      if (columnFieldSetters == null) {
        columnFieldSetters = Lists.newArrayList();
      }
      columnFieldSetters.add(new StreamingJsonParser.ActiveFieldInfo(fi));
    }
  }

  /**
   * Initialize the setters for generating the POJO
   */
  private void initializeActiveFieldSetters()
  {
    for (int i = 0; i < columnFieldSetters.size(); i++) {
      ActiveFieldInfo activeFieldInfo = columnFieldSetters.get(i);

      SupportType st = activeFieldInfo.fieldInfo.getType();

      switch (st) {
        case BOOLEAN:
          activeFieldInfo.setterOrGetter = PojoUtils.createSetterBoolean(getPojoClass(),
              activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;
        case DOUBLE:
          activeFieldInfo.setterOrGetter = PojoUtils.createSetterDouble(getPojoClass(),
              activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;
        case FLOAT:
          activeFieldInfo.setterOrGetter = PojoUtils.createSetterFloat(getPojoClass(),
              activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;
        case INTEGER:
          activeFieldInfo.setterOrGetter = PojoUtils.createSetterInt(getPojoClass(),
              activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;
        case STRING:
          activeFieldInfo.setterOrGetter = PojoUtils.createSetter(getPojoClass(),
              activeFieldInfo.fieldInfo.getPojoFieldExpression(), activeFieldInfo.fieldInfo.getType().getJavaType());
          break;
        case LONG:
          activeFieldInfo.setterOrGetter = PojoUtils.createSetterLong(getPojoClass(),
              activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;
        default:
          activeFieldInfo.setterOrGetter = PojoUtils.createSetter(getPojoClass(),
              activeFieldInfo.fieldInfo.getPojoFieldExpression(), Byte.class);
          break;
      }
      columnFieldSetters.get(i).setterOrGetter = activeFieldInfo.setterOrGetter;
    }
  }

  /**
   * Returns a POJO from a Generic Record Null is set as the default value if a
   * key is not found in the parsed JSON
   *
   * @return Object
   */
  @SuppressWarnings("unchecked")
  private Object setPojoFields(HashMap<Object, Object> tuple) throws InstantiationException, IllegalAccessException
  {
    Object newObj = getPojoClass().newInstance();
    try {
      for (int i = 0; i < columnFieldSetters.size(); i++) {

        StreamingJsonParser.ActiveFieldInfo afi = columnFieldSetters.get(i);
        SupportType st = afi.fieldInfo.getType();
        Object val = null;

        try {
          val = tuple.get(afi.fieldInfo.getColumnName());
        } catch (Exception e) {
          LOG.error("Could not find field -" + afi.fieldInfo.getColumnName() + "- in the generic record", e);
          val = null;
        }

        //Nothing to set if a value is null
        if (val == null) {
          continue;
        }

        try {
          switch (st) {
            case BOOLEAN:
              ((PojoUtils.SetterBoolean<Object>)afi.setterOrGetter).set(newObj,
                  (boolean)tuple.get(afi.fieldInfo.getColumnName()));
              break;
            case DOUBLE:
              ((PojoUtils.SetterDouble<Object>)afi.setterOrGetter).set(newObj,
                  (double)tuple.get(afi.fieldInfo.getColumnName()));
              break;
            case INTEGER:
              int intVal = Ints.checkedCast((long)tuple.get(afi.fieldInfo.getColumnName()));
              ((PojoUtils.SetterInt<Object>)afi.setterOrGetter).set(newObj, intVal);
              break;
            case STRING:
              ((PojoUtils.Setter<Object, String>)afi.setterOrGetter).set(newObj,
                  new String(tuple.get(afi.fieldInfo.getColumnName()).toString()));
              break;
            case LONG:
              ((PojoUtils.SetterLong<Object>)afi.setterOrGetter).set(newObj,
                  (long)tuple.get(afi.fieldInfo.getColumnName()));
              break;
            default:
              throw new RuntimeException("Invalid Support Type");
          }
        } catch (Exception e) {
          LOG.error("Exception in setting value", e);
          throw new RuntimeException(e);
        }

      }
    } catch (Exception ex) {
      LOG.error("Generic Exception in setting value" + ex.getMessage());
      newObj = null;
    }
    return newObj;
  }

  @OutputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<Object>()
  {
    public void setup(PortContext context)
    {
      jsonParser = new JSONParser();
      finder = new JsonKeyFinder();
      columnFields = new ArrayList<String>();
      columnFieldSetters = Lists.newArrayList();

      setPojoClass(context.getValue(Context.PortContext.TUPLE_CLASS));

      if (getFieldMappingString() == null) {
        setFieldInfos(createFieldInfoMap(generateFieldInfoInputs(getPojoClass())));
      } else {
        setFieldInfos(createFieldInfoMap(getFieldMappingString()));
      }
      initColumnFieldSetters(getFieldInfos());
      initializeActiveFieldSetters();

      ListIterator<FieldInfo> itr = fieldInfos.listIterator();
      while (itr.hasNext()) {
        columnFields.add(itr.next().getColumnName());
      }
      finder.setMatchKeyList(columnFields);
    }
  };

  private static final Logger LOG = LoggerFactory.getLogger(StreamingJsonParser.class);

}
