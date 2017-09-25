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
 * The partitioner package classes are responsible for creating the right number of Apex operators for a given kudu
 *  table that is being scanned. There are two configuration modes that are provided. Map one Kudu partition to
 *  one Apex operator or alternately map multiple Kudu tablets to one Apex operator. The bulk of the implementation
 *   is in the {@link org.apache.apex.malhar.kudu.partitioner.AbstractKuduInputPartitioner}.
 */
@InterfaceStability.Evolving
package org.apache.apex.malhar.kudu.partitioner;

import org.apache.hadoop.classification.InterfaceStability;
