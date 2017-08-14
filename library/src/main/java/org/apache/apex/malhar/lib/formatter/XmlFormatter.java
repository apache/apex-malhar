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
package org.apache.apex.malhar.lib.formatter;

import java.io.StringWriter;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.namespace.QName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * @displayName XmlParser
 * @category Formatter
 * @tags xml pojo formatter
 * @since 3.2.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class XmlFormatter extends Formatter<String>
{
  protected String alias;
  protected String dateFormat;
  protected boolean prettyPrint;

  private transient Marshaller marshaller;

  public XmlFormatter()
  {
    alias = null;
    dateFormat = null;
  }

  @Override
  public void setup(OperatorContext context)
  {
    JAXBContext ctx;
    try {
      ctx = JAXBContext.newInstance(getClazz());
      marshaller = ctx.createMarshaller();
      marshaller.setProperty(Marshaller.JAXB_FRAGMENT, true);
      marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, prettyPrint);
    } catch (JAXBException e) {
      DTThrowable.wrapIfChecked(e);
    }
  }

  @Override
  public String convert(Object tuple)
  {
    try {
      StringWriter writer = new StringWriter();
      if (getAlias() != null) {
        marshaller.marshal(new JAXBElement(new QName(getAlias()), tuple.getClass(), tuple), writer);
      } else {
        marshaller.marshal(new JAXBElement(new QName(getClazz().getSimpleName()), tuple.getClass(), tuple), writer);
      }
      return writer.toString();
    } catch (Exception e) {
      logger.debug("Error while converting tuple {} {} ", tuple, e.getMessage());
      return null;
    }
  }

  /**
   * Gets the alias This is an optional step. Without it XStream would work
   * fine, but the XML element names would contain the fully qualified name of
   * each class (including package) which would bulk up the XML a bit.
   *
   * @return alias.
   */
  public String getAlias()
  {
    return alias;
  }

  /**
   * Sets the alias This is an optional step. Without it XStream would work
   * fine, but the XML element names would contain the fully qualified name of
   * each class (including package) which would bulk up the XML a bit.
   *
   * @param alias
   *          .
   */
  public void setAlias(String alias)
  {
    this.alias = alias;
  }

  /**
   * Gets the date format e.g dd/mm/yyyy - this will be how a date would be
   * formatted
   *
   * @return dateFormat.
   */
  public String getDateFormat()
  {
    return dateFormat;
  }

  /**
   * Sets the date format e.g dd/mm/yyyy - this will be how a date would be
   * formatted
   *
   * @param dateFormat
   *          .
   */
  public void setDateFormat(String dateFormat)
  {
    this.dateFormat = dateFormat;
  }

  /**
   * Returns true if pretty print is enabled.
   *
   * @return prettyPrint
   */
  public boolean isPrettyPrint()
  {
    return prettyPrint;
  }

  /**
   * Sets pretty print option.
   *
   * @param prettyPrint
   */
  public void setPrettyPrint(boolean prettyPrint)
  {
    this.prettyPrint = prettyPrint;
  }

  private static final Logger logger = LoggerFactory.getLogger(XmlFormatter.class);

}
