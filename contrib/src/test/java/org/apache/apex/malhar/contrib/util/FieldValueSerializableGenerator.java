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
package org.apache.apex.malhar.contrib.util;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.util.FieldInfo;
import org.apache.apex.malhar.lib.util.FieldValueGenerator;
import org.apache.apex.malhar.lib.util.PojoUtils.Setter;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class FieldValueSerializableGenerator<T extends FieldInfo> extends FieldValueGenerator<T>
{

  public static <T extends FieldInfo> FieldValueSerializableGenerator<T> getFieldValueGenerator(final Class<?> clazz, List<T> fieldInfos)
  {
    return new FieldValueSerializableGenerator(clazz, fieldInfos);
  }

  private static final Logger logger = LoggerFactory.getLogger( FieldValueGenerator.class );
  //it's better to same kryo instance for both de/serialize
  private Kryo _kryo = null;
  private Class<?> clazz;

  private FieldValueSerializableGenerator(){}

  public FieldValueSerializableGenerator(Class<?> clazz, List<T> fieldInfos)
  {
    super(clazz, fieldInfos);
    this.clazz = clazz;
  }

  /**
   * get the object which is serialized.
   * this method will convert the object into a map from column name to column value and then serialize it
   *
   * @param obj
   * @return
   */
  public byte[] serializeObject( Object obj )
  {
  //if don't have field information, just convert the whole object to byte[]
    Object convertObj = obj;

    //if fields are specified, convert to map and then convert map to byte[]
    if ( fieldGetterMap != null && !fieldGetterMap.isEmpty() ) {
      convertObj = getFieldsValueAsMap(obj);
    }

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    Output output = new Output(os);

    getKryo().writeClassAndObject(output, convertObj);
    output.flush();
    //output.toBytes() is empty.
    return os.toByteArray();
  }


  public Object deserializeObject( byte[] bytes )
  {
    Object obj = getKryo().readClassAndObject( new Input( bytes ) );

    if ( fieldGetterMap == null || fieldGetterMap.isEmpty() ) {
      return obj;
    }

    // the obj in fact is a map, convert from map to object
    try {
      Map valueMap = (Map)obj;
      obj = clazz.newInstance();

      for ( Map.Entry<T, Setter<Object,Object>> entry : fieldSetterMap.entrySet() ) {
        T fieldInfo = entry.getKey();
        Setter<Object,Object> setter = entry.getValue();
        if (setter != null ) {
          setter.set(obj, valueMap.get( fieldInfo.getColumnName() ) );
        }
      }
      return obj;
    } catch ( Exception e ) {
      logger.warn( "Coverting map to obj exception. ", e );
      return obj;
    }
  }

  protected Kryo getKryo()
  {
    if ( _kryo == null ) {
      synchronized ( this ) {
        if ( _kryo == null ) {
          _kryo = new Kryo();
        }
        _kryo.setClassLoader(clazz.getClassLoader());
      }
    }
    return _kryo;
  }
}
