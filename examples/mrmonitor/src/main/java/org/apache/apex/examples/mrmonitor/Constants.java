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
package org.apache.apex.examples.mrmonitor;

/**
 * <p>Constants class.</p>
 *
 * @since 0.3.4
 */
public interface Constants
{

  public static final int MAX_NUMBER_OF_JOBS = 25;

  public static final String REDUCE_TASK_TYPE = "REDUCE";
  public static final String MAP_TASK_TYPE = "MAP";
  public static final String TASK_TYPE = "type";
  public static final String TASK_ID = "id";

  public static final String LEAGACY_TASK_ID = "taskId";
  public static final int MAX_TASKS = 2000;

  public static final String QUERY_APP_ID = "app_id";
  public static final String QUERY_JOB_ID = "job_id";
  public static final String QUERY_HADOOP_VERSION = "hadoop_version";
  public static final String QUERY_API_VERSION = "api_version";
  public static final String QUERY_RM_PORT = "rm_port";
  public static final String QUERY_HS_PORT = "hs_port";
  public static final String QUERY_HOST_NAME = "hostname";
  public static final String QUERY_KEY_COMMAND = "command";

}
