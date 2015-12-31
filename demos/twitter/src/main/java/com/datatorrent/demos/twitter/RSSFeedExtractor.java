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
 * RSSFeedExtractor class.
 * </p>
 *
 * @since 3.3.2
 */
public class RSSFeedExtractor implements InputOperator
{
  protected String URLToConvert = "";
  protected BufferedReader in;
  URL rssURL;
  DocumentBuilderFactory dbFactory;
  DocumentBuilder dBuilder;
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
  DocumentBuilderFactory URLdbFactory;
  DocumentBuilder URLdBuilder;
  
  @Override
  public void beginWindow(long windowId)
  {

  }

  @Override
  public void endWindow()
  {

  }

  @Override
  public void setup(OperatorContext context)
  {
    try {
      dbFactory = DocumentBuilderFactory.newInstance();
      dBuilder = dbFactory.newDocumentBuilder();
      InputStream is = this.getClass().getClassLoader()
          .getResourceAsStream("RSS.xml");
      Document doc = null;
      try {
        doc = dBuilder.parse(is);
      } catch (SAXException e) {
        throw new RuntimeException("SAX Exception", e);
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
      throw new RuntimeException("Parser Configuration Exception", e);

    } catch (IOException e) {
      throw new RuntimeException("Runtime Exception", e);
    }
  }

  @Override
  public void teardown()
  {

  }
  
  @Override
  public void emitTuples()
  {
      URLdbFactory = DocumentBuilderFactory.newInstance();
      try {
		URLdBuilder = URLdbFactory.newDocumentBuilder();
		Document page = URLdBuilder.parse(URLToConvert);
		NodeList itemList = page.getElementsByTagName("item");
		for (int i = 0; i < itemList.getLength(); i++){
		  Node it = itemList.item(i);
		  NodeList tagList = it.getChildNodes();
		  for (int j = 0; j < tagList.getLength(); j++) {
			  if (j % 2 != 0) {
				String tagName = tagList.item(j).getNodeName().toUpperCase();
				String tagValue = tagList.item(j).getTextContent();
			    System.out.println(tagName + " : " + tagValue);
			  }
		  }
		  System.out.println();
		  System.out.println("-----------------------------------");
		  System.out.println();
	    }
      } catch (ParserConfigurationException | SAXException | IOException e) {
	      throw new RuntimeException("Exception", e);
      }
  }
}
