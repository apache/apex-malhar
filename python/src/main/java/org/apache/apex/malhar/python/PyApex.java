/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.apex.malhar.python;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.log4j.PropertyConfigurator;

import py4j.GatewayServer;
import py4j.Py4JException;

public class PyApex
{

  private PythonApp streamApp = null;
  private static final Logger LOG = LoggerFactory.getLogger(PyApex.class);

  public PythonApp createApp(String name)
  {
    if (streamApp == null) {
      streamApp = new PythonApp(name);
    }
    return streamApp;
  }

  public PythonApp getAppByName(String name)
  {
    if (streamApp == null) {
      try {

        YarnClient client = YarnClient.createYarnClient();
        List<ApplicationReport> apps = client.getApplications();
        for (ApplicationReport appReport : apps) {
          if (appReport.getName().equals(name)) {
            LOG.debug("Application Name: {} Application ID: {} Application State: {}", appReport.getName(), appReport.getApplicationId().toString(), appReport.getYarnApplicationState());
            return new PythonApp(name, appReport.getApplicationId());
          }
        }
      } catch (Exception e) {
        throw new Py4JException("Error getting application list from resource manager", e);
      }
      streamApp = new PythonApp(name);
    }
    return streamApp;
  }

  public static void main(String[] args)
  {

    LOG.info("Starting PYAPEX with {}" + StringUtils.join(args, ' '));
    PropertyConfigurator.configure("./log.properties");
    PyApex pythonEntryPoint = new PyApex();
    GatewayServer gatewayServer = new GatewayServer(pythonEntryPoint);
    gatewayServer.start();
    LOG.debug("Gateway Server Started");
  }

}
