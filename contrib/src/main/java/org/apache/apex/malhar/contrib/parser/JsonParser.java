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

import java.io.IOException;
import java.util.Iterator;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.parser.Parser;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.hadoop.classification.InterfaceStability;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingMessage;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import com.google.common.annotations.VisibleForTesting;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * Operator that parses a json string tuple against a specified json schema and
 * emits JSONObject on one port, POJO on other port and tuples that could not be
 * parsed on error port.<br>
 * Schema is specified in a json format as per http://json-schema.org/ <br>
 * Example for the schema can be seen here http://json-schema.org/example1.html <br>
 * User can choose to skip validations by not specifying the schema at all. <br>
 * <br>
 * <b>Properties</b><br>
 * <b>jsonSchema</b>:schema as a string<br>
 * <b>clazz</b>:Pojo class <br>
 * <b>Ports</b> <br>
 * <b>in</b>:input tuple as a String. Each tuple represents a json string<br>
 * <b>parsedOutput</b>:tuples that are validated against the schema are emitted
 * as JSONObject on this port<br>
 * <b>out</b>:tuples that are validated against the schema are emitted as pojo
 * on this port<br>
 * <b>err</b>:tuples that do not confine to schema are emitted on this port as
 * KeyValPair<String,String><br>
 * Key being the tuple and Val being the reason.
 *
 *
 * @displayName JsonParser
 * @category Parsers
 * @tags json pojo parser
 * @since 3.2.0
 */
@InterfaceStability.Evolving
public class JsonParser extends Parser<byte[], KeyValPair<String, String>>
{

  /**
   * Contents of the schema.Schema is specified as per http://json-schema.org/
   */
  private String jsonSchema;
  private transient JsonSchema schema;
  private transient ObjectMapper objMapper;
  /**
   * output port to emit validate records as JSONObject
   */
  public transient DefaultOutputPort<JSONObject> parsedOutput = new DefaultOutputPort<JSONObject>();
  /**
   * metric to keep count of number of tuples emitted on {@link #parsedOutput}
   * port
   */
  @AutoMetric
  long parsedOutputCount;

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    parsedOutputCount = 0;
  }

  @Override
  public void setup(OperatorContext context)
  {
    try {
      if (jsonSchema != null) {
        JsonSchemaFactory factory = JsonSchemaFactory.byDefault();
        JsonNode schemaNode = JsonLoader.fromString(jsonSchema);
        schema = factory.getJsonSchema(schemaNode);
      }
      objMapper = new ObjectMapper();
      objMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    } catch (ProcessingException | IOException e) {
      DTThrowable.wrapIfChecked(e);
    }
  }

  @Override
  public void processTuple(byte[] tuple)
  {
    if (tuple == null) {
      if (err.isConnected()) {
        err.emit(new KeyValPair<String, String>(null, "null tuple"));
      }
      errorTupleCount++;
      return;
    }
    String incomingString = new String(tuple);
    try {
      if (schema != null) {
        ProcessingReport report = null;
        JsonNode data = JsonLoader.fromString(incomingString);
        report = schema.validate(data);
        if (report != null && !report.isSuccess()) {
          Iterator<ProcessingMessage> iter = report.iterator();
          StringBuilder s = new StringBuilder();
          while (iter.hasNext()) {
            ProcessingMessage pm = iter.next();
            s.append(pm.asJson().get("instance").findValue("pointer")).append(":").append(pm.asJson().get("message"))
                .append(",");
          }
          s.setLength(s.length() - 1);
          errorTupleCount++;
          if (err.isConnected()) {
            err.emit(new KeyValPair<String, String>(incomingString, s.toString()));
          }
          return;
        }
      }
      if (parsedOutput.isConnected()) {
        parsedOutput.emit(new JSONObject(incomingString));
        parsedOutputCount++;
      }
      if (out.isConnected()) {
        out.emit(objMapper.readValue(tuple, clazz));
        emittedObjectCount++;
      }
    } catch (JSONException | ProcessingException | IOException e) {
      errorTupleCount++;
      if (err.isConnected()) {
        err.emit(new KeyValPair<String, String>(incomingString, e.getMessage()));
      }
      logger.error("Failed to parse json tuple {}, Exception = {} ", tuple, e);
    }
  }

  @Override
  public Object convert(byte[] tuple)
  {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public KeyValPair<String, String> processErrorTuple(byte[] input)
  {
    throw new UnsupportedOperationException("Not supported");
  }

  /**
   * Get jsonSchema contents as a string to be used during validation
   *
   * @return jsonSchema
   */
  public String getJsonSchema()
  {
    return jsonSchema;
  }

  /**
   * Sets jsonSchema to be used during validation
   *
   * @param jsonSchema
   *          schema as a string
   */
  public void setJsonSchema(String jsonSchema)
  {
    this.jsonSchema = jsonSchema;
  }

  /**
   * Get errorTupleCount
   *
   * @return errorTupleCount
   */
  @VisibleForTesting
  public long getErrorTupleCount()
  {
    return errorTupleCount;
  }

  /**
   * Get emittedObjectCount
   *
   * @return emittedObjectCount
   */
  @VisibleForTesting
  public long getEmittedObjectCount()
  {
    return emittedObjectCount;
  }

  /**
   * Get incomingTuplesCount
   *
   * @return incomingTuplesCount
   */
  @VisibleForTesting
  public long getIncomingTuplesCount()
  {
    return incomingTuplesCount;
  }

  /**
   * Set schema.
   *
   * @param schema
   */
  @VisibleForTesting
  public void setSchema(JsonSchema schema)
  {
    this.schema = schema;
  }

  private static final Logger logger = LoggerFactory.getLogger(JsonParser.class);
}
