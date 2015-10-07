package com.datatorrent.contrib.enrichment;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.ClassUtils;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.ActivationListener;
import com.datatorrent.api.Sink;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;
import com.datatorrent.lib.util.PojoUtils.Setter;
import com.datatorrent.netlet.util.DTThrowable;
import com.esotericsoftware.kryo.NotNull;

/**
 * This class takes a POJO as input and extract the value of the lookupKey configured
 * for this operator. It then does a lookup in file/DB to find matching entry and all key-value pairs
 * specified in the file/DB or based on include fieldMap are added to original tuple.
 * This operator is App Builder schema support enabled. <br>
 * 
 * Properties:<br>
 * <b>inputClass</b>: Class to be loaded for the incoming data type<br>
 * <b>outputClass</b>: Class to be loaded for the emitted data type<br>
 * <br>
 *
 * Example
 * The file contains data in json format, one entry per line. during setup entire file is read and
 * kept in memory for quick lookup.
 * If file contains following lines, and operator is configured with lookup key "productId"
 * { "productId": 1, "productCategory": 3 }
 * { "productId": 4, "productCategory": 10 }
 * { "productId": 3, "productCategory": 1 }
 *
 * And input tuple is
 * { amount=10.0, channelId=4, productId=3 }
 *
 * The tuple is modified as below before operator emits it on output port.
 * { amount=10.0, channelId=4, productId=3, productCategory=1 }
 *
 * @displayName BeanEnrichment
 * @category Database
 * @tags enrichment, pojo, schema, lookup
 * @since 2.1.0
 */
@Evolving
public class POJOEnrichmentOperator extends AbstractEnrichmentOperator<Object, Object> implements ActivationListener<Context>
{
  private static final Logger log = LoggerFactory.getLogger(POJOEnrichmentOperator.class);

  private List<LookupKeyField> lookupKey = new ArrayList<LookupKeyField>();
  private List<EnrichField> fieldsToAddToOutputTuple = new ArrayList<EnrichField>();
  private List<CopyField> tupleFieldsToCopyFromInputToOutput = new ArrayList<CopyField>();
  
  @SuppressWarnings("rawtypes")
  private transient List<Getter> keyMethodMap;
  @SuppressWarnings("rawtypes")
  private transient List<Setter> resultMethodMap;
  private transient List<MethodMap> passOnFieldMethodMap;
  
  protected transient Class<?> inputClass;
  protected transient Class<?> outputClass;


  @InputPortFieldAnnotation(schemaRequired = true)
  public transient DefaultInputPort<Object> inputPojo = new DefaultInputPort<Object>() {

    @Override
    public void setup(PortContext context)
    {
      inputClass = context.getValue(Context.PortContext.TUPLE_CLASS);
    }

    @Override
    public void process(Object tuple)
    {
      POJOEnrichmentOperator.this.input.put(tuple);
    }
  };

  @OutputPortFieldAnnotation(schemaRequired = true)
  public transient DefaultOutputPort<Object> outputPojo = new DefaultOutputPort<Object>() {

    @Override
    public void setup(PortContext context)
    {
      outputClass = context.getValue(Context.PortContext.TUPLE_CLASS);
    }
  };

  public POJOEnrichmentOperator()
  {
    includeFieldsStr = "";
    lookupFieldsStr = "";
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  protected Object getKey(Object tuple)
  {
    ArrayList<Object> keyList = new ArrayList<Object>();

    for (Getter g : keyMethodMap) {
      keyList.add((Object) g.get(tuple));
    }
    
    return keyList;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  protected Object convert(Object in, Object cached)
  {
    Object out;

    try {
      out = outputClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      log.error("Failed to create instance of output schema.", e);
      return null;
    }

    for (MethodMap map : passOnFieldMethodMap) {
      try {
        map.set.set(out, map.get.get(in));
      }
      catch (RuntimeException e) {
        log.error("Failed to set the property. Continuing with default.", e);
      }
    }
    
    if (cached != null) {
      ArrayList<Object> newAttributes = (ArrayList<Object>) cached;
      int idx = 0;
      for (Setter s : resultMethodMap) {
        try {
          s.set(out, newAttributes.get(idx++));
        }
        catch (RuntimeException e) {
          log.error("Failed to set the property. Continuing with default.", e);
        }
      }
    }

    return out;
  }

  @Override
  public void setup(OperatorContext context)
  {
    boolean first = true;
    for (LookupKeyField map : lookupKey) {
      if (first) {
        lookupFieldsStr = map.getDbColumnName();
        first = false;
      }
      else {
        lookupFieldsStr += "," + map.getDbColumnName();
      }
    }
    
    first = true;
    for (EnrichField map : fieldsToAddToOutputTuple) {
      if (first) {
        includeFieldsStr = map.getFromDBColumn();
        first = false;
      }
      else {
        includeFieldsStr += "," + map.getFromDBColumn();
      }
    }
    
    Sink<Object> sink = new Sink<Object>() {
      @Override
      public void put(Object tuple)
      {
        outputPojo.emit(tuple);
      }

      @Override
      public int getCount(boolean reset)
      {
        return 0;
      }
    };

    super.output.setSink(sink);

    super.setup(context);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void activate(Context context)
  {
    keyMethodMap = new ArrayList<Getter>();
    for (LookupKeyField map : lookupKey) {
      try {
        keyMethodMap.add(generateGettersForField(inputClass, map.getInputPOJOSubstituteField()));
      } catch (NoSuchFieldException | SecurityException e) {
        throw new RuntimeException("Failed to initialize Input class getters for field: " + map.getInputPOJOSubstituteField(), e);
      }
    }

    resultMethodMap = new ArrayList<Setter>();
    for (EnrichField map : fieldsToAddToOutputTuple) {
      try {
        resultMethodMap.add(generateSettersForField(outputClass, map.getToOutputPOJOField()));
      } catch (NoSuchFieldException | SecurityException e) {
        throw new RuntimeException("Failed to initialize Output class getters for field: " + map.getToOutputPOJOField(), e);
      }
    }
    
    passOnFieldMethodMap = new ArrayList<MethodMap>();
    for (CopyField map : tupleFieldsToCopyFromInputToOutput) {
      if ((map.getFromInputPOJOField() == null) && (map.getToOutputPOJOField() == null)) {
        throw new RuntimeException("Both from & to fields cannot be null. Atleast one should be not null.");
      }

      MethodMap m = new MethodMap();
      try {
        m.get = generateGettersForField(inputClass, map.getFromInputPOJOField());
        m.set = generateSettersForField(outputClass, map.getToOutputPOJOField());
        passOnFieldMethodMap.add(m);
      } catch (NoSuchFieldException | SecurityException e) {
        throw new RuntimeException("Failed to initialize Input/Output class getters/setter.", e);
      }
    }
  }

  @Override
  public void deactivate()
  {
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private Setter generateSettersForField(Class<?> klass, String outputFieldName) throws NoSuchFieldException, SecurityException
  {
    Field f = outputClass.getDeclaredField(outputFieldName);
    Class c = ClassUtils.primitiveToWrapper(f.getType());
    Setter classSetter = PojoUtils.createSetter(klass, outputFieldName, c);
    return classSetter;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private Getter generateGettersForField(Class<?> klass, String inputFieldName) throws NoSuchFieldException, SecurityException
  {
    Field f = klass.getDeclaredField(inputFieldName);
    Class c = ClassUtils.primitiveToWrapper(f.getType());
    Getter classGetter = PojoUtils.createGetter(klass, inputFieldName, c);
    return classGetter;
  }

  /**
   * Specify the fields from the input tuple to be used to lookup the store. 
   * If the column names in the store are different from the tuple field names, specify the mappings below. 
   * Just specifying the tuple field name assumes that the column name is the same as the tuple field name
   * 
   * @return List of CompositeKeyField
   */
  public List<LookupKeyField> getLookupKey()
  {
    return lookupKey;
  }
  
  /**
   * Specify the fields from the input tuple to be used to lookup the store. 
   * If the column names in the store are different from the tuple field names, specify the mappings below. 
   * Just specifying the tuple field name assumes that the column name is the same as the tuple field name
   * 
   * @param List of CompositeKeyField
   * 
   * @description $[].inputPOJOSubstituteField Value from this input schema field will be substituted for database column
   * @description $[].dbColumnName This will be the name of the column which is part of composite key. This field is optional. If not provided, column name will be considered same as schema field name.
   * @useSchema $[].inputPOJOSubstituteField inputPojo.fields[].name
   */
  public void setLookupKey(List<LookupKeyField> lookupKey)
  {
    this.lookupKey = lookupKey;
  }

  /**
   * This sets lookup keys in string format. Following formats are allowed:
   * 
   *    {InputPOJOFieldName1}:{dbColumnName1},{InputPOJOFieldName2}:{dbColumnName2}
   *     - Field mapping will be set as it shows.
   *    {field1},{field2}
   *    - Both dbColumn and input POJO field name will be set to same value.
   *  
   * This property is supposed to be used only when property is set via conf files.
   * This property will not show up in UI.
   * 
   * @param lookup key in string format
   * @omitFromUI
   */
  public void setLookupKeyStr(String str)
  {
    String[] fieldSplit = str.split(",");
    for (String s : fieldSplit) {
      if (s.length() == 0) {
        continue;
      }
      String[] columnSplit = s.split(":");
      if (columnSplit.length == 0) {
        continue;
      }
      else {
        LookupKeyField field = new LookupKeyField();
        if (columnSplit.length == 1) {
          field.setInputPOJOSubstituteField(columnSplit[0]);
          field.setDbColumnName(columnSplit[0]);
        }
        else {
          field.setInputPOJOSubstituteField(columnSplit[0]);
          field.setDbColumnName(columnSplit[1]);
        }
        this.lookupKey.add(field);
      }
    }
  }
  

  /**
   * A list of fields to be enriched in output schema. 
   * Each item in the list gives mapping of database column to field in output schema.
   * 
   * @return List of EnrichField
   */
  public List<EnrichField> getFieldsToAddToOutputTuple()
  {
    return fieldsToAddToOutputTuple;
  }
  
  /**
   * A list of fields to be enriched in output schema. 
   * Each item in the list gives mapping of database column to field in output schema.
   * 
   * @param List of CompositeKeyField
   * 
   * @description $[].toOutputPOJOField Value from corresponding column in database will be set to this field in output schema.
   * @description $[].fromDBColumn Database column name for which value is to be retrieved. This field is optional. If not provided, column name will be considered same as schema field name.
   * @useSchema $[].toOutputPOJOField outputPojo.fields[].name
   */
  public void setFieldsToAddToOutputTuple(List<EnrichField> enrichedFields)
  {
    this.fieldsToAddToOutputTuple = enrichedFields;
  }
  

  /**
   * This sets fields to be added to output tuple in string format. Following formats are allowed:
   * 
   *    {fromDBColumnName1}:{toOutputPOJOField1},{fromDBColumnName2}:{toOutputPOJOField2}
   *     - Field mapping will be set as it shows.
   *    {field1},{field2}
   *    - Both dbColumn and input POJO field name will be set to same value.
   *  
   * This property is supposed to be used only when property is set via conf files.
   * This property will not show up in UI.
   * 
   * @param enriched key fields in string format
   * @omitFromUI
   */
  public void setFieldToAddToOutputTupleStr(String str)
  {
    String[] fieldSplit = str.split(",");
    for (String s : fieldSplit) {
      if (s.length() == 0) {
        continue;
      }
      String[] columnSplit = s.split(":");
      if (columnSplit.length == 0) {
        continue;
      }
      else {
        EnrichField field = new EnrichField();
        if (columnSplit.length == 1) {
          field.setFromDBColumn(columnSplit[0]);
          field.setToOutputPOJOField(columnSplit[0]);
        }
        else {
          field.setFromDBColumn(columnSplit[0]);
          field.setToOutputPOJOField(columnSplit[1]);
        }
        this.fieldsToAddToOutputTuple.add(field);
      }
    }
  }

  /**
   * A list of fields values to be copied from input to output tuple.
   * 
   * @return List of CopyField
   */
  public List<CopyField> getTupleFieldsToCopyFromInputToOutput()
  {
    return tupleFieldsToCopyFromInputToOutput;
  }

  /**
   * A list of fields values to be copied from input to output tuple.
   * 
   * @param List of CopyField
   * 
   * @description $[].fromInputPOJOField Source field in input schema. If not specified, same field name selected in Output schema will be considered.
   * @description $[].toOutputPOJOField Destination field in output schema. If not specified, same field name selected in Input schema will be considered.
   * @useSchema $[].fromInputPOJOField inputPojo.fields[].name
   * @useSchema $[].toOutputPOJOField outputPojo.fields[].name
   */
  public void setTupleFieldsToCopyFromInputToOutput(List<CopyField> copyFields)
  {
    this.tupleFieldsToCopyFromInputToOutput = copyFields;
  }
  
  
  /**
   * This sets fields to be copied from input to output tuple. Following formats are allowed:
   * 
   *    {fromInputPOJOField2}:{toOutputPOJOField1},{fromInputPOJOField2}:{toOutputPOJOField2}
   *     - Field mapping will be set as it shows.
   *    {field1},{field2}
   *    - Both input and output POJO field name will be set to same value.
   *  
   * This property is supposed to be used only when property is set via conf files.
   * This property will not show up in UI.
   * 
   * @param To be copied fields in string format
   * @omitFromUI
   */
  public void setTupleFieldsToCopyFromInputToOutputStr(String str)
  {
    String[] fieldSplit = str.split(",");
    for (String s : fieldSplit) {
      if (s.length() == 0) {
        continue;
      }
      String[] columnSplit = s.split(":");
      if (columnSplit.length == 0) {
        continue;
      }
      else {
        CopyField field = new CopyField();
        if (columnSplit.length == 1) {
          field.setFromInputPOJOField(columnSplit[0]);
          field.setToOutputPOJOField(columnSplit[0]);
        }
        else {
          field.setFromInputPOJOField(columnSplit[0]);
          field.setToOutputPOJOField(columnSplit[1]);
        }
        this.tupleFieldsToCopyFromInputToOutput.add(field);
      }
    }
  }

  /**
   * @omitFromUI
   */
  @Override
  public void setLookupFieldsStr(String lookupFieldsStr)
  {
    super.setLookupFieldsStr(lookupFieldsStr);
  }

  /**
   * @omitFromUI
   */
  @Override
  public void setIncludeFieldsStr(String includeFieldsStr)
  {
    super.setIncludeFieldsStr(includeFieldsStr);
  }

  /**
   * Returns the canonical name of the expected class on input port.
   * This property is required to be set from config file.
   *
   * @return Return input class canonical name
   * @omitFromUI
   */
  public String getInputClassStr()
  {
    return this.inputClass.getCanonicalName();
  }

  /**
   * Sets input class type expected on input port. The class should be found in classpath.
   *
   * @param inputClassStr
   * @omitFromUI
   */
  @SuppressWarnings("rawtypes")
  public void setInputClassStr(String inputClassStr)
  {
    try {
      this.inputClass = (Class)Thread.currentThread().getContextClassLoader().loadClass(inputClassStr);
    } catch (Throwable cause) {
      DTThrowable.rethrow(cause);
    }
  }

  /**
   * Returns the canonical name of the output class on output port.
   *
   * @return Returns output class canonical name.
   * @omitFromUI
   */
  public String getOutputClassStr()
  {
    return this.outputClass.getCanonicalName();
  }

  /**
   * Sets output class type to be returns on output port. This class should be found in classpath.
   * This property is required to be set from config file.
   *
   * @param outputClassStr
   * @omitFromUI
   */
  @SuppressWarnings("rawtypes")
  public void setOutputClassStr(String outputClassStr)
  {
    try {
      this.outputClass = (Class)Thread.currentThread().getContextClassLoader().loadClass(outputClassStr);
    } catch (Throwable cause) {
      DTThrowable.rethrow(cause);
    }
  }

  public static class LookupKeyField
  {
    @NotNull
    private String inputPOJOSubstituteField;
    private String dbColumnName;

    public LookupKeyField() 
    {
      // Required by kryo
    }
    public String getInputPOJOSubstituteField()
    {
      return inputPOJOSubstituteField;
    }
    public void setInputPOJOSubstituteField(String inputPOJOSubstituteField)
    {
      this.inputPOJOSubstituteField = inputPOJOSubstituteField;
      if (this.dbColumnName == null) this.dbColumnName = inputPOJOSubstituteField;
    }
    public String getDbColumnName()
    {
      return dbColumnName;
    }
    public void setDbColumnName(String dbColumnName)
    {
      this.dbColumnName = dbColumnName;
    }
  }
  
  public static class EnrichField
  {
    @NotNull
    private String toOutputPOJOField;
    private String fromDBColumn;
    public EnrichField() 
    {
      // Required by kryo
    }
    public String getToOutputPOJOField()
    {
      return toOutputPOJOField;
    }
    public void setToOutputPOJOField(String toOutputPOJOField)
    {
      this.toOutputPOJOField = toOutputPOJOField;
      if (this.fromDBColumn == null) this.fromDBColumn = toOutputPOJOField;
    }
    public String getFromDBColumn()
    {
      return fromDBColumn;
    }
    public void setFromDBColumn(String fromDBColumn)
    {
      this.fromDBColumn = fromDBColumn;
    }
  }
  
  public static class CopyField
  {
    private String fromInputPOJOField;
    private String toOutputPOJOField;
    public CopyField() 
    {
      // Required by kryo
    }
    public String getFromInputPOJOField()
    {
      return fromInputPOJOField;
    }
    public void setFromInputPOJOField(String fromInputPOJOField)
    {
      this.fromInputPOJOField = fromInputPOJOField;
      if (this.toOutputPOJOField == null) this.toOutputPOJOField = fromInputPOJOField;
    }
    public String getToOutputPOJOField()
    {
      return toOutputPOJOField;
    }
    public void setToOutputPOJOField(String toOutputPOJOField)
    {
      this.toOutputPOJOField = toOutputPOJOField;
      if (this.fromInputPOJOField == null) this.fromInputPOJOField = toOutputPOJOField;
    }
  }

  @SuppressWarnings("rawtypes")
  public static class MethodMap
  {
    public Getter get;
    public Setter set;
  }
}
