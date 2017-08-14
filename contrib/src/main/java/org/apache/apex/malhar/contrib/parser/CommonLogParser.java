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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.contrib.parser.log.CommonLog;
import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Context;

/**
 * Operator that parses a log string tuple against the
 * a specified json schema and emits POJO on a parsed port and tuples that could not be
 * parsed on error port.<br>
 * <b>Properties</b><br>
 * <b>jsonSchema</b>:schema as a string<br>
 * <b>pojoClass</b>:Pojo class in case of user specified schema<br>
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
@InterfaceStability.Evolving
public class CommonLogParser extends LogParser
{
  private static final Logger logger = LoggerFactory.getLogger(CommonLogParser.class);

  private String schema = "{\n" +
      "  \"fields\": [{\n" +
      "    \"field\": \"host\",\n" +
      "    \"regex\": \"^([0-9.]+)\"\n" +
      "  }, {\n" +
      "    \"field\": \"rfc931\",\n" +
      "    \"regex\": \"(\\\\S+)\"\n" +
      "  }, {\n" +
      "    \"field\": \"username\",\n" +
      "    \"regex\": \"(\\\\S+)\"\n" +
      "  }, {\n" +
      "    \"field\": \"datetime\",\n" +
      "    \"regex\": \"\\\\[(.*?)\\\\]\"\n" +
      "  },{\n" +
      "    \"field\": \"request\",\n" +
      "    \"regex\": \"\\\"((?:[^\\\"]|\\\")+)\\\"\"\n" +
      "  },{\n" +
      "    \"field\": \"statusCode\",\n" +
      "    \"regex\": \"(\\\\d{3})\"\n" +
      "  },{\n" +
      "    \"field\": \"bytes\",\n" +
      "    \"regex\": \"(\\\\d+|-)\"\n" +
      "  }]\n" +
      "}";


  @Override
  public void setup(Context.OperatorContext context)
  {
    this.setLogFileFormat(schema);
    super.setup(context);
    super.setClazz(CommonLog.class);
  }


}
