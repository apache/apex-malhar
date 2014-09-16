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

/**
 * <p>
 * This is an abstract operator, which parses incoming tuples using the Java XML DOM parser and passes the resultant Document object along with
 * the tuple to the extending operator for processing.
 * <p>
 *
 * @displayName Abstract Xml DOM Operator
 * @category xml
 * @tags
 *
 * @since 1.0.4
 * @deprecated
 */
@Deprecated
public abstract class XmlDOMOperator<T> extends AbstractXmlDOMOperator<T>
{
}
