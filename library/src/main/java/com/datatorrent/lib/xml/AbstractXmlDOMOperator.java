/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.xml;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.DTThrowable;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

/**
 * This is the base implementation for an xml operator,
 * which parses incoming tuples using the Java XML DOM parser.&nbsp;
 * Subclasses must implement the methods which are use to process the parsed XML.
 * <p></p>
 * @displayName Abstract XML DOM
 * @category xml
 * @tags
 *
 * @since 1.0.2
 */
public abstract class AbstractXmlDOMOperator<T> extends BaseOperator
{
  protected transient DocumentBuilderFactory docFactory;
  protected transient DocumentBuilder docBuilder;

  @Override
  public void setup(Context.OperatorContext context)
  {
    try {
      docFactory = DocumentBuilderFactory.newInstance();
      docBuilder = docFactory.newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      throw new RuntimeException(e);
    }
  }

  public transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      processTuple(tuple);
    }
  };

  protected void processTuple(T tuple) {
    try {
      InputSource source = getInputSource(tuple);
      Document document = docBuilder.parse(source);
      processDocument(document, tuple);
    } catch (Exception e) {
      DTThrowable.rethrow(e);
    }
  }

  protected abstract InputSource getInputSource(T tuple);

  protected abstract void processDocument(Document document, T tuple);

}
