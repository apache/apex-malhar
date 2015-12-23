/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.twitter;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.InputOperator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * <p>
 * TwitterStatusUserExtractor class.
 * </p>
 *
 * @since 0.3.2
 */
public class RSSFeedExtractor implements InputOperator
{
  protected String URLToConvert = "";
  protected BufferedReader in;
  URL rssURL;
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

  @Override
  public void beginWindow(long windowId)
  {
    // TODO Auto-generated method stub
  }

  @Override
  public void endWindow()
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void setup(OperatorContext context)
  {
    try {
      DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
      InputStream is = this.getClass().getClassLoader()
          .getResourceAsStream("RSS.xml");
      Document doc = null;
      try {
        doc = dBuilder.parse(is);
      } catch (SAXException e) {
        throw new RuntimeException("SAX Exception");
      }

      doc.getDocumentElement().normalize();

      NodeList nList = doc.getElementsByTagName("sub");

      for (int index = 0; index < nList.getLength(); index++) {
        Node nNode = nList.item(index);
        if (nNode.getNodeType() == Node.ELEMENT_NODE) {
          Element eElement = (Element) nNode;
          URLToConvert = eElement.getElementsByTagName("value").item(0)
              .getTextContent();
        }
      }

    } catch (ParserConfigurationException e) {
      throw new RuntimeException("Parser Configuration Exception");

    } catch (IOException e) {
      throw new RuntimeException("Runtime Exception");
    }

    try {
      rssURL = new URL(URLToConvert);
    } catch (MalformedURLException e) {
      throw new RuntimeException("Malformed URL Exception");
    }
  }

  @Override
  public void teardown()
  {
    // TODO Auto-generated method stub
    try {
      in.close();
    } catch (IOException e) {
      throw new RuntimeException("IO Exception");
    }
  }

  @Override
  public void emitTuples()
  {
    // TODO Auto-generated method stub
    try {
      in = new BufferedReader(new InputStreamReader(rssURL.openStream()));
      String sourceCode = "";
      String line;
      while ((line = in.readLine()) != null) {
        if (line.contains("<title>")) {
          int firstIndex = line.indexOf("<title>");
          String index = line.substring(firstIndex);
          index = index.replace("<title>", "");
          int lastIndex = index.indexOf("</title>");
          index = index.substring(0, lastIndex);
          sourceCode += index + "\n";
        }
        if (line.contains("<link>")) {
          int firstIndex = line.indexOf("<link>");
          String index = line.substring(firstIndex);
          index = index.replace("<link>", "");
          int lastIndex = index.indexOf("</link>");
          index = index.substring(0, lastIndex);
          sourceCode += index + "\n";
        }
      }
      in.close();
      output.emit(sourceCode);
    } catch (MalformedURLException e) {
      throw new RuntimeException("Malformed URL Exception");
    } catch (IOException e) {
      throw new RuntimeException("Runtime Exception");
    }
    try {
      in.close();
    } catch (IOException e) {
      throw new RuntimeException("IO Exception");
    }

  }

}
