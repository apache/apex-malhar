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

import java.util.ArrayList;
import java.util.List;

import javax.validation.constraints.NotNull;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import org.apache.apex.malhar.lib.xml.AbstractXmlDOMOperator;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * An operator that performs a cartesian product between different elements in a xml document.
 * <p>
 * The cartesian product is performed between two sets of elements. The elements are specified
 * using xpath. The resultant product contains the values of the elements, Multiple
 * cartesian products can be specified in a single operator.
 *
 * The cartesian product sets are specified using the config parameter. The configuration
 * specification is as follows:
 *
 * 1. Product: A cartesian product can be specified as follows
 *
 *      a1,a2:b1,b2,b3
 *
 *      Here a1 denotes the xpath of the first element in the first set,
 *      b1 denotes the xpath of the first element of second set and so on.
 *      In the above example a cartesian product is specified between two sets one
 *      containing 2 elements and the other containing 3 elements. In practice
 *      any number of elements can be specified in either set. Notice the delimiter ':'
 *      between the two sets.
 *
 *      The result of the cartesian product is a collection of all the combinations of the values
 *      of the elements of the two sets taking one from each set at a time. The value of an element is customizable
 *      and can be specified by extending the operator. For example if the element is a leaf element its text value
 *      could be used in the value. If we denote the value of an element using the notation v[element], in the above
 *      example the product would be the collection of the following elements (v[a1] v[b1]), (v[a1] v[b2]),
 *      (v[a1] v[b3]), (v[a2] v[b1]), (v[a2] v[b2]) and (v[a2] v[b3])
 *
 *      The format of the elements in the product can be customized by implementing the abstract methods when extending
 *      the operator.
 *
 * 2. Grouping: Sometimes a subset of the elements need to be grouped together. This operator supports grouping.
 *    The group is treated as a single element when computing the product and the elements in the group are kept
 *    together. A group can be specified as follows
 *
 *    a1,a2:(b1,b2),b3
 *
 *    Groups are specified using a single opening and closing brace. In the above example b1 and b2 are grouped
 *    together and treated as a single element when computing the product. In this case the result of the product would
 *    be (v[a1] v[b1] v[b2]), (v[a2] v[b1] v[b2]), (v[a1] v[b3]), and (v[a2] v[b3]). Notice that v[b1] and v[b2] are
 *    not separated and go together. Grouping can be specified in either sets or both.
 *
 * 3. Section: In some cases there are more than one parent elements with the same tag name in the xml
 *    document and a product needs to be performed with the children of each parent. This is sectional
 *    specification and is done as follows
 *
 *    a#(b1,b2:c1,c2)
 *
 *    The parent elements are specified using an absolute xpath, in the above example 'a'. The child elements
 *    are specified using relative paths from the parent element, The parent and child specification are
 *    separated by a delimited '#'.
 *
 *    In the above example all the parent elements with xpath 'a' are retrieved and for each element the cartesian
 *    product is performed on the specified children. In this case for every 'a' element the product collection
 *    (v[a/b1] v[a/c1]). (v[a/b1] v[a/c2]). (v[a/b2] v[a/c1]). (v[a/b2] v[a/c2]) is computed. All the product
 *    collections are accumulated into a result collection.
 *
 *    Grouping can also be used inside sectional specification. In the above example the b children can be grouped
 *    as follows
 *
 *    a#((b1, b2):c1,c2)
 *
 * 4. Element shortcut: If a element that does not contain a value directly is specified but it has child elements
 *    that have values then it is automatically substituted with all those child elements that have values traversing
 *    till the leaf elements. For example if the xml document is
 *
 *        <a>
 *          <b>
 *            <c>...</c>
 *            <d>
 *              <e>...</e>
 *            </d>
 *          </b>
 *          <f>...</f>
 *          <g>...</g>
 *        </a>
 *
 *    and c, e, f and g have values. If the configuration is specified as
 *
 *    b:f,g
 *
 *    the element b is substituted with its children elements that have values namely c and e. The resulting
 *    configuration is
 *
 *    c,e:f,g
 *
 *    and the product would be (v[c] v[f]), (v[c] v[g]), (v[e] v[f]) and (v[e] v[g])
 *
 *    Grouping can also be specified for a parent element in which case all the children elements of that parent
 *    element that have values are grouped together.
 *
 * 5. Multiple products: More than one cartesian product can be specified. They need to be separated using a
 *    delimiter '|'. Each product is computed independent of each other, An example of this is
 *
 *    a1,a2:b1,b2|c1,c2:d1,d2|e1,e2,e3:f1
 *
 * @displayName Abstract XML Cartesian Product
 * @category Math
 * @tags cartesian product, xml, multiple products, dom operator
 * @since 1.0.1
 */
public abstract class AbstractXmlCartesianProduct<T> extends AbstractXmlDOMOperator<T>
{
  @NotNull
  private String config;

  private transient XPath xpath;

  private transient PathElementFactory pathElementFactory;
  private transient CartesianProductFactory cartesianProductFactory;

  protected void processDocument(Document document, T tuple)
  {
    try {
      List<String> result = new ArrayList<String>();
      for (CartesianProduct cartesianProduct : cartesianProducts) {
        cartesianProduct.product(document, result);
      }
      processResult(result, tuple);
    } catch (XPathExpressionException e) {
      DTThrowable.rethrow(e);
    }
  }

  protected abstract void processResult(List<String> result, T tuple);

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    xpath = XPathFactory.newInstance().newXPath();
    pathElementFactory = new PathElementFactory();
    cartesianProductFactory = new CartesianProductFactory();
    parseConfig();
  }

  public void setConfig(String config)
  {
    this.config = config;
  }

  public void parseConfig()
  {
    String[] strprods = config.split("\\|");
    cartesianProducts = new CartesianProduct[strprods.length];
    for (int i = 0; i < strprods.length; ++i) {
      cartesianProducts[i] = cartesianProductFactory.getSpecable(strprods[i]);
    }
  }

  private interface Specable
  {
    public void parse(String spec);
  }

  private interface SpecableFactory<T>
  {
    public T getSpecable(String spec);
  }

  public interface PathElement extends Specable
  {

    public void productRight(Node context, PathElement pathElement, List<String> result) throws XPathExpressionException;

    public void productLeft(Node context, String value, List<String> result) throws XPathExpressionException;

    public List<Node> getValueNodes(Node node) throws XPathExpressionException;

  }

  public class SimplePathElement implements PathElement
  {
    public String path;

    public void parse(String spec)
    {
      path = spec;
    }

    @Override
    public void productRight(Node context, PathElement pathElement, List<String> result) throws XPathExpressionException
    {
      List<Node> nodes = AbstractXmlCartesianProduct.this.getValueNodes(context, path);
      for (Node node : nodes) {
        String value = getValue(node);
        pathElement.productLeft(context, value, result);
      }
    }

    @Override
    public void productLeft(Node context, String left, List<String> result) throws XPathExpressionException
    {
      List<Node> nodes = AbstractXmlCartesianProduct.this.getValueNodes(context, path);
      for (Node node : nodes) {
        String value = getValue(node);
        String product = product(left, value);
        result.add(product);
      }
    }

    @Override
    public List<Node> getValueNodes(Node context) throws XPathExpressionException
    {
      return AbstractXmlCartesianProduct.this.getValueNodes(context, path);
    }
  }

  public class GroupPathElement implements PathElement
  {
    public boolean unified;
    public PathElement[] pathElements;

    public void parse(String spec)
    {
      String estr = spec;
      if (spec.length() >= 2) {
        // Check if it is a unified element, it can have nested elements inside
        if (spec.charAt(0) == '(') {
          int balance = 1;
          int i;
          for (i = 1; (i < spec.length()) && (balance > 0); ++i) {
            if (spec.charAt(i) == ')') {
              balance--;
            } else if (spec.charAt(i) == '(') {
              balance++;
            }
          }
          if (i == spec.length()) {
            estr = spec.substring(1, spec.length() - 1);
            unified = true;
          }
        }
      }
      String[] selements = estr.split(",");
      pathElements = new PathElement[selements.length];
      for (int i = 0; i < selements.length; ++i) {
        pathElements[i] = pathElementFactory.getSpecable(selements[i]);
      }
    }

    @Override
    public void productRight(Node context, PathElement pathElement, List<String> result) throws XPathExpressionException
    {
      if (!unified) {
        for (PathElement ePathElement : pathElements) {
          ePathElement.productRight(context, pathElement, result);
        }
      } else {
        List<Node> nodes = getValueNodes(context);
        String value = getValue(nodes);
        pathElement.productLeft(context, value, result);
      }
    }

    @Override
    public void productLeft(Node context, String value, List<String> result) throws XPathExpressionException
    {
      if (!unified) {
        for (PathElement pathElement : pathElements) {
          pathElement.productLeft(context, value, result);
        }
      } else {
        List<Node> nodes = getValueNodes(context);
        String evalue = getValue(nodes);
        String product = product(value, evalue);
        result.add(product);
      }
    }

    @Override
    public List<Node> getValueNodes(Node context) throws XPathExpressionException
    {
      List<Node> nodes = new ArrayList<Node>();
      for (PathElement pathElement : pathElements) {
        nodes.addAll(pathElement.getValueNodes(context));
      }
      return nodes;
    }
  }

  private interface CartesianProduct extends Specable
  {
    public void parse(String productSpec);

    public void product(Node context, List<String> result) throws XPathExpressionException;
  }

  public class RegularCartesianProduct implements CartesianProduct
  {
    public PathElement element1;
    public PathElement element2;


    @Override
    public void parse(String spec)
    {
      String[] elements = spec.split("\\:");
      if (elements.length == 2) {
        element1 = pathElementFactory.getSpecable(elements[0]);
        element2 = pathElementFactory.getSpecable(elements[1]);
      }
    }

    @Override
    public void product(Node context, List<String> result) throws XPathExpressionException
    {
      element1.productRight(context, element2, result);
    }
  }

  public class SelectionCartesianProduct implements CartesianProduct
  {
    public SimplePathElement parentElement;
    public PathElement childElement1;
    public PathElement childElement2;

    @Override
    public void parse(String productSpec)
    {
      int seltnDelIdx = productSpec.indexOf("#");
      if (seltnDelIdx != -1) {
        String parentSpec = productSpec.substring(0, seltnDelIdx);

        PathElement pathElement = pathElementFactory.getSpecable(parentSpec);
        if (SimplePathElement.class.isAssignableFrom(pathElement.getClass())) {
          if (productSpec.length() > (seltnDelIdx + 3)) {
            int chldStDelIdx = seltnDelIdx + 1;
            int chldEdDelIdx = productSpec.length() - 1;
            int chldSepDelIdx;
            if ((productSpec.charAt(chldStDelIdx) == '(') && (productSpec.charAt(chldEdDelIdx) == ')')
                && ((chldSepDelIdx = productSpec.indexOf(':')) != -1)) {
              String child1Spec = productSpec.substring(chldStDelIdx + 1, chldSepDelIdx);
              String child2Spec = productSpec.substring(chldSepDelIdx + 1, chldEdDelIdx);
              parentElement = (SimplePathElement)pathElement;
              childElement1 = pathElementFactory.getSpecable(child1Spec);
              childElement2 = pathElementFactory.getSpecable(child2Spec);
            }
          }
        }
      }
    }

    @Override
    public void product(Node context, List<String> result) throws XPathExpressionException
    {
      NodeList nodes = getNodes(context, parentElement.path);
      for (int i = 0; i < nodes.getLength(); ++i) {
        childElement1.productRight(nodes.item(i), childElement2, result);
      }
    }
  }

  private class PathElementFactory implements SpecableFactory<PathElement>
  {

    @Override
    public PathElement getSpecable(String spec)
    {
      PathElement pathElement = null;
      if (spec.matches("[^,(]*")) {
        pathElement = new SimplePathElement();
      } else {
        pathElement = new GroupPathElement();
      }
      pathElement.parse(spec);
      return pathElement;
    }
  }

  private class CartesianProductFactory implements SpecableFactory<CartesianProduct>
  {

    @Override
    public CartesianProduct getSpecable(String spec)
    {
      CartesianProduct product = null;
      if (spec.indexOf("#") == -1) {
        product = new RegularCartesianProduct();
      } else {
        product = new SelectionCartesianProduct();
      }
      if (product != null) {
        product.parse(spec);
      }
      return product;
    }
  }

  private List<Node> getNodes(Document document, String path) throws XPathExpressionException
  {
    XPathExpression pathExpr = xpath.compile(path);
    NodeList nodeList = (NodeList)pathExpr.evaluate(document, XPathConstants.NODESET);
    List<Node> nodes = new ArrayList<Node>();
    for (int i = 0; i < nodeList.getLength(); ++i) {
      nodes.add(nodeList.item(i));
    }
    return nodes;
  }

  protected List<Node> getValueNodes(Node node, String path) throws XPathExpressionException
  {
    NodeList nodeList = getNodes(node, path);
    List<Node> nodes = new ArrayList<Node>();
    getValueNodes(nodeList, nodes);
    return nodes;
  }

  private NodeList getNodes(Node node, String path) throws XPathExpressionException
  {
    XPathExpression pathExpr = xpath.compile(path);
    return (NodeList)pathExpr.evaluate(node, XPathConstants.NODESET);
  }

  protected void getValueNodes(NodeList nodes, List<Node> textNodes)
  {
    for (int i = 0; i < nodes.getLength(); ++i) {
      Node node = nodes.item(i);
      if (isValueNode(node)) {
        textNodes.add(node);
      } else {
        getValueNodes(node.getChildNodes(), textNodes);
      }
    }
  }

  public String getValue(List<Node> nodes)
  {
    StringBuilder sb = new StringBuilder();
    String delim = getDelim();
    boolean first = true;
    for (Node node : nodes) {
      if (!first) {
        sb.append(delim);
      } else {
        first = false;
      }
      sb.append(getValue(node));
    }
    return sb.toString();
  }

  public String getDelim()
  {
    return ",";
  }

  public String product(String left, String right)
  {
    StringBuilder sb = new StringBuilder();
    sb.append(left).append(getDelim()).append(right);
    return sb.toString();
  }

  protected abstract boolean isValueNode(Node node);

  protected abstract String getValue(Node node);

  private transient CartesianProduct[] cartesianProducts;
}
