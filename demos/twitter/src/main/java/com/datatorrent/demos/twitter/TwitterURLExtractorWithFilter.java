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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

import twitter4j.Status;
import twitter4j.URLEntity;

/**
 * <p>
 * TwitterStatusURLExtractorWithFilter class.
 * </p>
 *
 * @since 0.3.2
 */
public class TwitterURLExtractorWithFilter extends BaseOperator
{

  protected List<String> myList = new ArrayList<String>();
  BufferedReader in;

  public final transient DefaultOutputPort<String> url = new DefaultOutputPort<String>();
  public final transient DefaultInputPort<Status> input = new DefaultInputPort<Status>()
  {
    @Override
    public void process(Status status)
    {
      if (myList.isEmpty()) {
        try {
          DocumentBuilderFactory dbFactory = DocumentBuilderFactory
              .newInstance();
          DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
          InputStream is = this.getClass().getClassLoader()
              .getResourceAsStream("domainlist.xml");
          Document doc = dBuilder.parse(is);
          doc.getDocumentElement().normalize();
          NodeList nList = doc.getElementsByTagName("sub");

          for (int temp = 0; temp < nList.getLength(); temp++) {
            Node nNode = nList.item(temp);
            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
              Element eElement = (Element) nNode;
              String items = eElement.getElementsByTagName("value").item(0)
                  .getTextContent();
              myList = Arrays.asList(items.split(", "));
            }
          }

        } catch (ParserConfigurationException e) {
          throw new RuntimeException("Parser Configuration Exception");
        } catch (IOException e) {
          throw new RuntimeException("Runtime Exception");
        } catch (SAXException e) {
          throw new RuntimeException("SAX Exception");
        }

      }

      URLEntity[] entities = status.getURLEntities();
      if (entities != null) {
        for (URLEntity ue : entities) {
          if (ue != null) { // see why we intermittently get NPEs
            String toEmit = (ue.getExpandedURL() == null ? ue.getURL() : ue
                .getExpandedURL()).toString();
            for (String word : myList) {
              if (toEmit.contains(word)) {
                url.emit(toEmit);
              }
            }
          }
        }
      }
    }
  };
}
