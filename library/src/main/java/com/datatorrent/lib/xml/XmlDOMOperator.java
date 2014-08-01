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
 * Created by Pramod Immaneni <pramod@datatorrent.com> on 6/12/14.
 *
 * @since 1.0.2
 */
public abstract class XmlDOMOperator<T> extends BaseOperator
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
