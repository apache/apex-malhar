/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr;

import com.google.common.collect.Maps;
import java.lang.annotation.Annotation;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class ResultSerializerFactory
{
  private static final Logger logger = LoggerFactory.getLogger(ResultSerializerFactory.class);

  private Map<Class<? extends Result>, CustomResultSerializer> typeToCustomResultBuilder = Maps.newHashMap();

  public ResultSerializerFactory()
  {
  }

  public String serialize(Result result)
  {
    CustomResultSerializer mcrs = typeToCustomResultBuilder.get(result.getClass());
    Class<? extends Result> schema = result.getClass();

    if(mcrs == null) {
      Annotation[] ans = schema.getAnnotations();

      Class<? extends CustomResultSerializer> crs = null;

      for(Annotation an: ans) {
        if(an instanceof ResultSerializerInfo) {
          if(crs != null) {
            throw new UnsupportedOperationException("Cannot specify the "
                    + ResultSerializerInfo.class
                    + " annotation twice on the class: "
                    + schema);
          }

          crs = ((ResultSerializerInfo)an).clazz();
        }
      }

      if(crs == null) {
        throw new UnsupportedOperationException("No " + ResultSerializerInfo.class
                + " annotation found on class: "
                + schema);
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

      typeToCustomResultBuilder.put(schema, mcrs);
    }

    return mcrs.serialize(result);
  }
}
