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
package org.apache.apex.malhar.lib.parser;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import org.apache.apex.malhar.lib.util.ReusableStringReader;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * Operator that converts XML string to Pojo <br>
 * <b>Properties</b> <br>
 * <b>alias</b>:This maps to the root element of the XML string. If not
 * specified, parser would expect the root element to be fully qualified name of
 * the Pojo Class. <br>
 * <b>dateFormats</b>: Comma separated string of date formats e.g
 * dd/mm/yyyy,dd-mmm-yyyy where first one would be considered default
 *
 * @displayName XmlParser
 * @category Parsers
 * @tags xml pojo parser
 * @since 3.2.0
 */
@InterfaceStability.Evolving
public class XmlParser extends Parser<String, String> implements Operator.ActivationListener<Context>
{
  private String schemaXSDFile;
  private transient Unmarshaller unmarshaller;
  private transient Validator validator;
  private transient Schema schema;
  private ReusableStringReader reader = new ReusableStringReader();
  public transient DefaultOutputPort<Document> parsedOutput = new DefaultOutputPort<Document>();

  @Override
  public Object convert(String tuple)
  {
    // This method is not invoked for XML parser
    return null;
  }

  @Override
  public void processTuple(String inputTuple)
  {
    try {
      if (out.isConnected()) {
        reader.open(inputTuple);
        JAXBElement<?> output = unmarshaller.unmarshal(new StreamSource(reader), getClazz());
        LOG.debug(output.getValue().toString());
        emittedObjectCount++;
        out.emit(output.getValue());
      } else if (validator != null) {
        validator.validate(new StreamSource(inputTuple));
      }
      if (parsedOutput.isConnected()) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder;
        try {
          builder = factory.newDocumentBuilder();
          Document doc = builder.parse(new InputSource(new ByteArrayInputStream(inputTuple.getBytes("UTF-8"))));
          parsedOutput.emit(doc);

        } catch (Exception e) {
          LOG.info("Failed to parse xml tuple {}, Exception = {} , StackTrace = {}", inputTuple, e, e.getStackTrace());
          errorTupleCount++;
          if (err.isConnected()) {
            err.emit(inputTuple);
          }
        }
      }
    } catch (Exception e) {
      LOG.info("Failed to parse xml tuple {}, Exception = {}, StackTrace = {} ", inputTuple, e, e.getStackTrace());
      errorTupleCount++;
      if (err.isConnected()) {
        err.emit(inputTuple);
      }
    } finally {
      try {
        if (reader.isOpen()) {
          reader.close();
        }
      } catch (IOException e) {
        DTThrowable.wrapIfChecked(e);
      }
    }
  }

  @Override
  public String processErrorTuple(String input)
  {
    return input;
  }

  @Override
  public void setup(com.datatorrent.api.Context.OperatorContext context)
  {
    try {
      if (schemaXSDFile != null) {
        Path filePath = new Path(schemaXSDFile);
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.newInstance(filePath.toUri(), configuration);
        FSDataInputStream inputStream = fs.open(filePath);

        SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        schema = factory.newSchema(new StreamSource(inputStream));
        validator = schema.newValidator();
        fs.close();
      }
    } catch (SAXException e) {
      DTThrowable.wrapIfChecked(e);
    } catch (IOException e) {
      DTThrowable.wrapIfChecked(e);
    }
  }

  public String getSchemaFile()
  {
    return schemaXSDFile;
  }

  public void setSchemaFile(String schemaFile)
  {
    this.schemaXSDFile = schemaFile;
  }

  public static Logger LOG = LoggerFactory.getLogger(Parser.class);

  @Override
  public void activate(Context context)
  {
    try {
      JAXBContext ctx = JAXBContext.newInstance(getClazz());
      unmarshaller = ctx.createUnmarshaller();
      if (schemaXSDFile != null) {
        unmarshaller.setSchema(schema);
      }
    } catch (JAXBException e) {
      DTThrowable.wrapIfChecked(e);
    }
  }

  @Override
  public void deactivate()
  {

  }
}
