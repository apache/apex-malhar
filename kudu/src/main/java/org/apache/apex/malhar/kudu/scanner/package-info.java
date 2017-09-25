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
 * The scanner package holds classes responsible scanning kudu tablets as per the provided predicates. There are two
 *  types of scanner model provided. {@link org.apache.apex.malhar.kudu.scanner.KuduPartitionConsistentOrderScanner}
 *   provides for a consistent ordering of tuples. This is to be used when exactly once processing semantics are
 *   required in the downstream operators. The second model is
 *   {@link org.apache.apex.malhar.kudu.scanner.KuduPartitionRandomOrderScanner} provides for a throughput oriented
 *   implementation and cannot be used when exactly once semantics is required in the downstream operators.
 */
@InterfaceStability.Evolving
package org.apache.apex.malhar.kudu.scanner;

import org.apache.hadoop.classification.InterfaceStability;
