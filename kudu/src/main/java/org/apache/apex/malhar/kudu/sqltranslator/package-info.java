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
/***
 * The sqltranslator package classes are responsible for constructing Kudu predicates given a SQL expression
 *  as input. The main approach to build Kudu predicates is by implementing a tree listener that gets
 *  callbacks from the Antlr parser engine basing on the grammar annotations. There is also an Error Listener to
 *  help in marking a given SQL expression with the appropriate error markers so that they can be used in the code
 *  to identify the exact error causes.
 */
@InterfaceStability.Evolving
package org.apache.apex.malhar.kudu.sqltranslator;

import org.apache.hadoop.classification.InterfaceStability;
