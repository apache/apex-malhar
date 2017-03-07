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
package org.apache.apex.malhar.python;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;

import py4j.GatewayServer;
import py4j.Py4JException;

public class PyApex
{

  private PythonApp streamApp = null;
  private static final Logger LOG = LoggerFactory.getLogger(PyApex.class);
  private PythonAppManager.LaunchMode mode = null;

  public PyApex(PythonAppManager.LaunchMode mode)
  {
    this.mode = mode;
  }

  public PythonApp createApp(String name)
  {
    if (streamApp == null) {
      streamApp = new PythonApp(name);
      if (this.mode != null) {
        streamApp.setMode(this.mode);
      }
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

    LOG.info("Starting PYAPEX with {}", StringUtils.join(args, ' '));
    Options options = new Options();

    Option input = new Option("m", "mode", true, "Launch Mode");
    input.setRequired(false);
    options.addOption(input);

    Option pyfile = new Option("l", "launch-file", true, "Launch file");
    pyfile.setRequired(false);
    options.addOption(pyfile);

    CommandLineParser parser = new BasicParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      LOG.error("Parsing Exception while parsing arguments",e);
      formatter.printHelp("utility-name", options);
      System.exit(1);
      return;
    }

    String launchModeValue = cmd.getOptionValue("mode");
    PythonAppManager.LaunchMode mode = launchModeValue != null ? PythonAppManager.LaunchMode.valueOf(launchModeValue) : null;
    LOG.info("Starting PYAPEX with {}", mode);
    PyApex pythonEntryPoint = new PyApex(mode);
    GatewayServer gatewayServer = new GatewayServer(pythonEntryPoint);
    gatewayServer.start();
    LOG.debug("Gateway Server Started");
  }

}
