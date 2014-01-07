/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.stream;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

import javax.validation.constraints.NotNull;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.*;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;

import com.datatorrent.lib.parser.CsvParserOperator.ReusableStringReader;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * Takes xml byte array and picks values of specific nodes and attributes using xpath and emits them as a map of key values
 * <b>input</b>: expects xml byte array<br>
 * <b>outputMap</b>: emits HashMap&lt;String,String&gt;<br>
 * key: user supplied key
 * value: value of the key from xml
 *
 * @Since 0.9.3
 */
public class XMLParseOperator extends BaseOperator
{
  private transient DocumentBuilderFactory builderFactory;
  private transient DocumentBuilder builder;
  private transient XPath xPath;
  /*
   * user defined map for xpath lookup
   * key: name of the element
   * value: element xpath XPathExpression to lookup
   */
  @NotNull
  private Map<String, XPathExpression> elementKeys;
  private Map<String, XPathExpression> attributeKeys;
  private HashMap<String, String> keyMap;
  private static final Logger logger = LoggerFactory.getLogger(XMLParseOperator.class);
  private InputSource inputSource;
  private ReusableStringReader reader;

  @Override
  public void setup(OperatorContext context)
  {
    try {
      builderFactory = DocumentBuilderFactory.newInstance();
      builder = builderFactory.newDocumentBuilder();
      xPath = XPathFactory.newInstance().newXPath();
      elementKeys = new HashMap<String, XPathExpression>();
      attributeKeys = new HashMap<String, XPathExpression>();
      keyMap = new HashMap<String, String>();
      inputSource = new InputSource();
      reader = new ReusableStringReader();
      inputSource.setCharacterStream(reader);
    }
    catch (ParserConfigurationException ex) {
      logger.error("setup exception", ex);
    }
  }

  /*
   * xml input as a byte array
   */
  @InputPortFieldAnnotation(name = "byteArrayXMLInput")
  public final transient DefaultInputPort<byte[]> byteArrayXMLInput = new DefaultInputPort<byte[]>()
  {
    @Override
    public void process(byte[] t)
    {
      processTuple(t);
      outputMap.emit(keyMap);
    }

  };

  /**
   * Gets the values of required elements from xml and emits them
   *
   * @param xmlBytes
   */
  private void processTuple(byte[] xmlBytes)
  {
    keyMap = new HashMap<String, String>();
    String xml = new String(xmlBytes);
    try {
      // node value
      for (Map.Entry<String, XPathExpression> entry : elementKeys.entrySet()) {
        XPathExpression expression = entry.getValue();
        String key = entry.getKey();
        reader.open(xml);
        Node node = (Node)expression.evaluate(inputSource, XPathConstants.NODE);
        keyMap.put(key, node.getTextContent());
      }

      // attribute value
      for (Map.Entry<String, XPathExpression> entry : attributeKeys.entrySet()) {
        XPathExpression expression = entry.getValue();
        String key = entry.getKey();
        reader.open(xml);
        String attr = (String)expression.evaluate(inputSource, XPathConstants.STRING);
        keyMap.put(key, attr);
      }
    }
    catch (XPathExpressionException ex) {
      logger.error("error in xpath", ex);
    }
    catch (IOException ex) {
      logger.error("exception during converting xml string to document", ex);
    }


  }

  /**
   * keys to extract from xml using xpath
   *
   * @param element xpath key
   * @param name name to use for key
   */
  public void addElementKeys(String name, String element)
  {
    try {
      XPathExpression expression = xPath.compile(element);
      if (element.contains("@")) {
        elementKeys.put(name, expression);
      }
      else {
        attributeKeys.put(name, expression);
      }
    }
    catch (XPathExpressionException ex) {
      logger.error("error compiling xpath", ex);
    }
  }

  /**
   * Output hash map port.
   */
  @OutputPortFieldAnnotation(name = "map")
  public final transient DefaultOutputPort<HashMap<String, String>> outputMap = new DefaultOutputPort<HashMap<String, String>>();
}
