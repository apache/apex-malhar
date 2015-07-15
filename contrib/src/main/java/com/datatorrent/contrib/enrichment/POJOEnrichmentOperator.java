package com.datatorrent.contrib.enrichment;

import com.datatorrent.api.Context;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;
import com.datatorrent.lib.util.PojoUtils.Setter;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.esotericsoftware.kryo.NotNull;
import org.apache.commons.lang3.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * This class takes a POJO as input and extract the value of the lookupKey configured
 * for this operator. It then does a lookup in file/DB to find matching entry and all key-value pairs
 * specified in the file/DB or based on include fieldMap are added to original tuple.
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
 * @tags enrichment, lookup
 *
 * @since 2.1.0
 */
public class POJOEnrichmentOperator extends AbstractEnrichmentOperator<Object, Object> {

  private transient static final Logger logger = LoggerFactory.getLogger(POJOEnrichmentOperator.class);
  protected Class inputClass;
  protected Class outputClass;
  private transient List<Getter> getters = new LinkedList<Getter>();
  private transient List<FieldObjectMap> fieldMap = new LinkedList<FieldObjectMap>();
  private transient List<Setter> updateSetter = new LinkedList<Setter>();

  @NotNull
  protected String outputClassStr;


  @Override
  protected Object getKey(Object tuple) {
    ArrayList<Object> keyList = new ArrayList<Object>();
    for(Getter g : getters) {
        keyList.add(g.get(tuple));
    }
    return keyList;
  }

  @Override
  protected Object convert(Object in, Object cached) {
    try {
      Object o = outputClass.newInstance();

      // Copy the fields from input to output
      for (FieldObjectMap map : fieldMap) {
        map.set.set(o, map.get.get(in));
      }

      if (cached == null)
        return o;

      if(updateSetter.size() == 0 && includeFields.size() != 0) {
        populateUpdatesFrmIncludeFields();
      }
      ArrayList<Object> newAttributes = (ArrayList<Object>)cached;
      int idx = 0;
      for(Setter s: updateSetter) {
        s.set(o, newAttributes.get(idx));
        idx++;
      }
      return o;
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setup(Context.OperatorContext context) {
    super.setup(context);
    populateUpdatesFrmIncludeFields();
  }

  private void populateGettersFrmLookup()
  {
    for (String fName : lookupFields) {
        Getter f = PojoUtils.createGetter(inputClass, fName, Object.class);
        getters.add(f);
    }
  }

  private void populateGettersFrmInput()
  {
    Field[] fields = inputClass.getFields();
    for (Field f : fields) {
      Class c = ClassUtils.primitiveToWrapper(f.getType());
      FieldObjectMap fieldMap = new FieldObjectMap();
      fieldMap.get = PojoUtils.createGetter(inputClass, f.getName(), c);
      try {
        fieldMap.set = PojoUtils.createSetter(outputClass, f.getName(), c);
      } catch (Throwable e) {
        throw new RuntimeException("Failed to initialize Output Class for field: " + f.getName(), e);
      }
      this.fieldMap.add(fieldMap);
    }
  }

  private void populateUpdatesFrmIncludeFields() {
    if (this.outputClass == null) {
      logger.debug("Creating output class instance from string: {}", outputClassStr);
      try {
        this.outputClass = this.getClass().getClassLoader().loadClass(outputClassStr);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }

    for (String fName : includeFields) {
      try {
        Field f = outputClass.getField(fName);
        Class c;
        if(f.getType().isPrimitive()) {
          c = ClassUtils.primitiveToWrapper(f.getType());
        } else {
           c = f.getType();
        }
        try {
          updateSetter.add(PojoUtils.createSetter(outputClass, f.getName(), c));
        } catch (Throwable e) {
          throw new RuntimeException("Failed to initialize Output Class for field: " + f.getName(), e);
        }
      } catch (NoSuchFieldException e) {
        throw new RuntimeException("Cannot find field '" + fName + "' in output class", e);
      }
    }
  }

  public String getOutputClassStr()
  {
    return outputClassStr;
  }

  public void setOutputClassStr(String outputClassStr)
  {
    this.outputClassStr = outputClassStr;
  }

  @Override protected void processTuple(Object tuple)
  {
    if (inputClass == null) {
      inputClass = tuple.getClass();
      populateGettersFrmLookup();
      populateGettersFrmInput();
    }
    super.processTuple(tuple);
  }

  private class FieldObjectMap
  {
    public Getter get;
    public Setter set;
  }
}
