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
package com.datatorrent.lib.appdata.query.serde;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.lang.annotation.Annotation;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

public class MessageDeserializerFactory
{
  private static final Logger logger = LoggerFactory.getLogger(MessageDeserializerFactory.class);

  private final Map<String, Class<? extends Message>> typeToClass = Maps.newHashMap();
  private final Map<String, CustomMessageDeserializer> typeToCustomQueryBuilder = Maps.newHashMap();
  private final Map<String, CustomMessageValidator> typeToCustomQueryValidator = Maps.newHashMap();
  private final Map<Class<? extends Message>, Object> deserializationContext = Maps.newHashMap();

  public MessageDeserializerFactory(Class<? extends Message>... schemas)
  {
    setClasses(schemas);
  }

  public void setContext(Class<? extends Message> clazz,
                         Object context)
  {
    deserializationContext.put(clazz, context);
  }

  private void setClasses(Class<? extends Message>[] schemas)
  {
    Preconditions.checkArgument(schemas.length != 0, "No schemas provided.");

    Set<Class<? extends Message>> clazzes = Sets.newHashSet();

    for(Class<? extends Message> schema: schemas)
    {
      Preconditions.checkNotNull(schema, "Provided schema cannot be null");
      Preconditions.checkArgument(!clazzes.contains(schema), "Schema %s was passed twice.", schema);
      clazzes.add(schema);

      Annotation[] ans = schema.getAnnotations();

      String schemaType = null;
      Class<? extends CustomMessageDeserializer> cqd = null;
      Class<? extends CustomMessageValidator> cqv = null;

      for(Annotation an: ans)
      {
        if(an instanceof MessageType) {
          if(schemaType != null) {
            throw new IllegalArgumentException("Cannot specify the " + MessageType.class +
              " annotation twice on the class: " + schema);
          }

          schemaType = ((MessageType) an).type();

          logger.debug("Detected schemaType for {} is {}",
                       schema,
                       schemaType);
        }
        else if(an instanceof MessageDeserializerInfo) {
          if(cqd != null) {
            throw new IllegalArgumentException("Cannot specify the " + MessageDeserializerInfo.class +
              " annotation twice on the class: " + schema);
          }

          cqd = ((MessageDeserializerInfo) an).clazz();
        }
        else if(an instanceof MessageValidatorInfo) {
          if(cqv != null) {
            throw new IllegalArgumentException("Cannot specify the " + MessageValidatorInfo.class +
              " annotation twice on the class: ");
          }

          cqv = ((MessageValidatorInfo) an).clazz();
        }
      }

      if(schemaType == null) {
        throw new IllegalArgumentException("No " + MessageType.class + " annotation found on class: " + schema);
      }

      if(cqd == null) {
        throw new IllegalArgumentException("No " + MessageDeserializerInfo.class + " annotation found on class: " +
          schema);
      }

      if(cqv == null) {
        throw new IllegalArgumentException("No " + MessageValidatorInfo.class + " annotation found on class: " + schema);
      }

      Class<? extends Message> prevSchema = typeToClass.put(schemaType, schema);
      logger.debug("prevSchema {}:", prevSchema);

      if(prevSchema != null) {
        throw new IllegalArgumentException("Cannot have the " +
          schemaType + " schemaType defined on multiple classes: " + schema + ", " + prevSchema);
      }

      try {
        CustomMessageDeserializer cqdI = cqd.newInstance();
        CustomMessageValidator cqvI = cqv.newInstance();
        cqdI.setDataClazz(schema);
        typeToCustomQueryBuilder.put(schemaType, cqdI);
        typeToCustomQueryValidator.put(schemaType, cqvI);
      }
      catch(InstantiationException ex) {
        throw new RuntimeException(ex);
      }
      catch(IllegalAccessException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  public Message deserialize(String json) throws IOException
  {
    String type;

    try
    {
      JSONObject jsonObject = new JSONObject(json);
      type = jsonObject.getString(Message.FIELD_TYPE);
    }
    catch(JSONException e)
    {
      throw new IOException(e);
    }

    CustomMessageDeserializer cqb = typeToCustomQueryBuilder.get(type);

    if(cqb == null) {
      throw new IOException("The query type " +
                            type +
                            " does not have a corresponding deserializer.");
    }

    CustomMessageValidator cqv = typeToCustomQueryValidator.get(type);
    Object context = deserializationContext.get(typeToClass.get(type));
    Message data = cqb.deserialize(json, context);

    logger.debug("{}", data);

    if(data == null || !(cqv != null && cqv.validate(data, context))) {
      return null;
    }

    data.setType(type);
    return data;
  }
}
