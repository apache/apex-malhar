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
package org.apache.apex.malhar.kafka;

public class KafkaConsumerPropertiesTest extends AbstractKafkaConsumerPropertiesTest
{
  @Override
  public AbstractKafkaInputOperator createKafkaInputOperator()
  {
    return new KafkaSinglePortInputOperator();
  }

  @Override
  public String expectedException()
  {
    //return new String("org.apache.kafka.common.KafkaException: Jaas configuration not found");
    return new String("java.lang.IllegalArgumentException: Could not find a 'KafkaClient' entry in the JAAS configuration. System property 'java.security.auth.login.config' is not set");
  }
}
