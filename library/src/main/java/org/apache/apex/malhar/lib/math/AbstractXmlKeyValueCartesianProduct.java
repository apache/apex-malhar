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
package org.apache.apex.malhar.lib.math;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * This operator extends the AbstractXmlCartesianProduct operator and implements the node value
 * as a key value pair of node name and the node's text value.
 *
 * @displayName Abstract XML Key Value Cartesian Product
 * @category Math
 * @tags cartesian product, xml, multiple products, key value
 * @since 1.0.1
 */
public abstract class AbstractXmlKeyValueCartesianProduct<T> extends AbstractXmlCartesianProduct<T>
{
  @Override
  public String getValue(Node node)
  {
    return node.getNodeName() + "=" + getTextValue(node);
  }

  @Override
  public boolean isValueNode(Node n)
  {
    return isTextContainerNode(n);
  }

  private boolean isTextContainerNode(Node n)
  {
    boolean container = false;
    NodeList childNodes = n.getChildNodes();
    if (childNodes.getLength() == 1) {
      Node firstChild = childNodes.item(0);
      if (firstChild.getNodeType() == Node.TEXT_NODE) {
        container = true;
      }
    }
    return container;
  }

  protected String getTextValue(Node n)
  {
    String text = null;
    NodeList childNodes = n.getChildNodes();
    if (childNodes.getLength() == 1) {
      Node firstChild = childNodes.item(0);
      text = firstChild.getNodeValue();
    }
    return text;
  }
}
