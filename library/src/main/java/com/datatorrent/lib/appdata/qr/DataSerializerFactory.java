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
package com.datatorrent.lib.appdata.qr;

import com.datatorrent.lib.appdata.schemas.ResultFormatter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.lang.annotation.Annotation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DataSerializerFactory
{
  private static final Logger logger = LoggerFactory.getLogger(DataSerializerFactory.class);

  private Map<Class<? extends Result>, CustomDataSerializer> clazzToCustomResultBuilder = Maps.newHashMap();
  private Map<Class<? extends Result>, String> clazzToType = Maps.newHashMap();

  private ResultFormatter appDataFormatter = new ResultFormatter();

  public DataSerializerFactory(ResultFormatter resultFormatter)
  {
    setResultFormatter(resultFormatter);
  }

  private void setResultFormatter(ResultFormatter resultFormatter)
  {
    this.appDataFormatter = Preconditions.checkNotNull(resultFormatter);
  }

  public ResultFormatter getResultFormatter()
  {
    return appDataFormatter;
  }

  public String serialize(Result result)
  {
    CustomDataSerializer mcrs = clazzToCustomResultBuilder.get(result.getClass());
    Class<? extends Result> schema = result.getClass();

    if(mcrs == null) {
      Annotation[] ans = schema.getAnnotations();

      Class<? extends CustomDataSerializer> crs = null;
      String type = null;

      for(Annotation an: ans) {
        if(an instanceof DataSerializerInfo) {
          if(crs != null) {
            throw new UnsupportedOperationException("Cannot specify the "
                    + DataSerializerInfo.class
                    + " annotation twice on the class: "
                    + schema);
          }

          crs = ((DataSerializerInfo)an).clazz();
        }
        else if(an instanceof DataType) {
          if(type != null) {
            throw new UnsupportedOperationException("Cannot specify the " +
                                                    DataType.class +
                                                    " annotation twice on the class: " +
                                                    schema);
          }

          type = ((DataType) an).type();
        }
      }

      if(crs == null) {
        throw new UnsupportedOperationException("No " + DataSerializerInfo.class
                + " annotation found on class: "
                + schema);
      }

      if(type == null) {
        throw new UnsupportedOperationException("No " + DataType.class +
                                                " annotation found on class " + schema);
      }

      try {
        mcrs = crs.newInstance();
      }
      catch(InstantiationException ex) {
        throw new RuntimeException(ex);
      }
      catch(IllegalAccessException ex) {
        throw new RuntimeException(ex);
      }

      clazzToCustomResultBuilder.put(schema, mcrs);
      clazzToType.put(schema, type);
    }

    result.setType(clazzToType.get(schema));

    return mcrs.serialize(result, appDataFormatter);
  }
}
