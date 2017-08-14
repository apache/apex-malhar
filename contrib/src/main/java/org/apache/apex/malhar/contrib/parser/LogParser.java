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
import javax.validation.constraints.NotNull;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.parser.Parser;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.commons.lang3.CharEncoding;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceStability;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;

/**
 * Operator that parses a log string tuple against the
 * a specified json schema and emits POJO on a parsed port and tuples that could not be
 * parsed on error port.<br>
 * <b>Properties</b><br>
 * <b>jsonSchema</b>:schema as a string<br>
 * <b>clazz</b>:Pojo class in case of user specified schema<br>
 * <b>Ports</b> <br>
 * <b>in</b>:input tuple as a String. Each tuple represents a log<br>
 * <b>parsedOutput</b>:tuples that are validated against the specified schema are emitted
 * as POJO on this port<br>
 * <b>err</b>:tuples that do not confine to log format are emitted on this port as
 * KeyValPair<String,String><br>
 * Key being the tuple and Val being the reason.
 *
 * @since 3.7.0
 */
@InterfaceStability.Unstable
public class LogParser extends Parser<byte[], KeyValPair<String, String>>
{
  private transient Class<?> clazz;

  @NotNull
  private String logFileFormat;

  private String encoding;

  private LogSchemaDetails logSchemaDetails;

  private transient ObjectMapper objMapper;

  @Override
  public Object convert(byte[] tuple)
  {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public KeyValPair<String, String> processErrorTuple(byte[] bytes)
  {
    return null;
  }

  /**
   * output port to emit valid records as POJO
   */
  public transient DefaultOutputPort<Object> parsedOutput = new DefaultOutputPort<Object>()
  {
    public void setup(Context.PortContext context)
    {
      clazz = context.getValue(Context.PortContext.TUPLE_CLASS);
    }
  };

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
  public void setup(Context.OperatorContext context)
  {
    objMapper = new ObjectMapper();
    encoding = encoding != null ? encoding : CharEncoding.UTF_8;
    setupLog();
  }

  @Override
  public void processTuple(byte[] inputTuple)
  {
    if (inputTuple == null) {
      this.emitError(null, "null tuple");
      return;
    }
    String incomingString = "";
    try {
      incomingString = new String(inputTuple, encoding);
      if (StringUtils.isBlank(incomingString)) {
        this.emitError(incomingString, "Blank tuple");
        return;
      }
      logger.debug("Input string {} ", incomingString);
      logger.debug("Parsing with log format {}", this.geLogFileFormat());
      if (this.logSchemaDetails != null && clazz != null) {
        if (parsedOutput.isConnected()) {
          parsedOutput.emit(objMapper.readValue(this.logSchemaDetails.createJsonFromLog(incomingString).toString().getBytes(), clazz));
          parsedOutputCount++;
        }
      }
    } catch (NullPointerException | IOException | JSONException e) {
      this.emitError(incomingString, e.getMessage());
      logger.error("Failed to parse log tuple {}, Exception = {} ", inputTuple, e);
    }
  }

  /**
   * Emits error on error port
   * @param tuple
   * @param errorMsg
   */
  public void emitError(String tuple, String errorMsg)
  {
    if (err.isConnected()) {
      err.emit(new KeyValPair<String, String>(tuple, errorMsg));
    }
    errorTupleCount++;
  }

  /**
   *  Setup for the logs according to the logFileFormat
   */
  public void setupLog()
  {
    try {
      //parse the schema from logFileFormat string
      this.logSchemaDetails = new LogSchemaDetails(logFileFormat);
    } catch (IllegalArgumentException e) {
      logger.error("Error while initializing the custom log format " + e.getMessage());
    }
  }

  /**
   * Set log file format required for parsing the log
   * @param logFileFormat
   */
  public void setLogFileFormat(String logFileFormat)
  {
    this.logFileFormat = logFileFormat;
  }

  /**
   * Get log file format required for parsing the log
   * @return logFileFormat
   */
  public String geLogFileFormat()
  {
    return logFileFormat;
  }

  /**
   * Get encoding parameter for converting tuple into String
   * @return logSchemaDetails
   */
  public String getEncoding()
  {
    return encoding;
  }

  /**
   * Set encoding parameter for converting tuple into String
   * @param encoding
   */
  public void setEncoding(String encoding)
  {
    this.encoding = encoding;
  }

  /**
   * Get log schema details (field, regex etc)
   * @return logSchemaDetails
   */
  public LogSchemaDetails getLogSchemaDetails()
  {
    return logSchemaDetails;
  }

  /**
   * Set log schema details like (fields and regex)
   * @param logSchemaDetails
   */
  public void setLogSchemaDetails(LogSchemaDetails logSchemaDetails)
  {
    this.logSchemaDetails = logSchemaDetails;
  }

  /**
   * Get the class that needs to be formatted
   * @return Class<?>
   */
  public Class<?> getClazz()
  {
    return clazz;
  }

  /**
   * Set the class of tuple that needs to be formatted
   * @param clazz
   */
  public void setClazz(Class<?> clazz)
  {
    this.clazz = clazz;
  }

  private static final Logger logger = LoggerFactory.getLogger(LogParser.class);
}
