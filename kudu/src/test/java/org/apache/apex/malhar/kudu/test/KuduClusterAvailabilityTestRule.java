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
package org.apache.apex.malhar.kudu.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.kudu.BaseKuduOutputOperator;
import org.apache.apex.malhar.kudu.IncrementalStepScanInputOperator;
import org.apache.apex.malhar.kudu.KuduClientTestCommons;
import org.apache.apex.malhar.kudu.KuduInputOperatorCommons;

/**
 * A Junit rule that helps in bypassing tests that cannot be done if the kudu cluster is not present.
 */
public class KuduClusterAvailabilityTestRule implements TestRule
{

  private static final transient Logger LOG = LoggerFactory.getLogger(KuduClusterAvailabilityTestRule.class);

  public static final String KUDU_MASTER_HOSTS_STRING_ENV = "kudumasterhosts";
  @Override
  public Statement apply(Statement base, Description description)
  {
    KuduClusterTestContext testContext = description.getAnnotation(KuduClusterTestContext.class);
    String masterHostsValue = System.getProperty(KUDU_MASTER_HOSTS_STRING_ENV);
    boolean runThisTest = true; // default is to run the test if no annotation is specified
    if ( testContext != null) {
      if ((masterHostsValue != null) && (testContext.kuduClusterBasedTest())) {
        runThisTest = true;
      }
      if ((masterHostsValue != null) && (!testContext.kuduClusterBasedTest())) {
        runThisTest = false;
      }
      if ((masterHostsValue == null) && (testContext.kuduClusterBasedTest())) {
        runThisTest = false;
      }
      if ((masterHostsValue == null) && (!testContext.kuduClusterBasedTest())) {
        runThisTest = true;
      }
    }
    if (runThisTest) {
      // call the before class equivalent for real kudu cluster
      if ((testContext != null ) && (testContext.kuduClusterBasedTest())) {
        try {
          initRealKuduClusterSetup(masterHostsValue);
        } catch (Exception e) {
          throw new RuntimeException("Not able to initialize kudu cluster",e);
        }
      } else {
        // call the before class equivalent for mocked kudu cluster
        try {
          KuduInputOperatorCommons.initCommonConfigsForAllTypesOfTests();
        } catch (Exception e) {
          throw new RuntimeException("Could not initialize commn configs required for Kudu cluster connectivity",e);
        }
      }
      // Run the original test
      return base;
    } else {
      // bypass the test altogether
      return new Statement()
      {

        @Override
        public void evaluate() throws Throwable
        {
          // Return an empty Statement object for those tests
        }
      };
    }

  }

  private void initRealKuduClusterSetup(String masterHosts) throws Exception
  {
    if (!KuduClientTestCommons.tableInitialized) {
      synchronized (KuduClientTestCommons.objectForLocking) { // test rigs can be parallelized
        if (!KuduClientTestCommons.tableInitialized) {
          KuduClientTestCommons.setKuduMasterAddresses(masterHosts);
          KuduClientTestCommons.setup();
          initPropertiesFilesForBaseKuduOutputOperator(masterHosts);
          initPropertiesFilesForIncrementalStepScanInputOperator(masterHosts);
          KuduClientTestCommons.tableInitialized = true;
        }
      }
    }
  }

  private void initPropertiesFilesForBaseKuduOutputOperator(String masterHosts)
  {
    BaseKuduOutputOperator.DEFAULT_CONNECTION_PROPS_FILE_NAME = rewritePropsFile(
      masterHosts,BaseKuduOutputOperator.DEFAULT_CONNECTION_PROPS_FILE_NAME);
  }

  private void initPropertiesFilesForIncrementalStepScanInputOperator(String masterHosts)
  {
    IncrementalStepScanInputOperator.DEFAULT_CONNECTION_PROPS_FILE_NAME = rewritePropsFile(
      masterHosts,IncrementalStepScanInputOperator.DEFAULT_CONNECTION_PROPS_FILE_NAME);
  }

  /**
   * Creates a temporary properties file that sets the kudu cluster master hosts value as given in the command line.
   * @param masterHosts
   * @param originalFileName File name as would be picked by the class loader
   * @return
   */
  private String rewritePropsFile(String masterHosts, String originalFileName)
  {
    Properties kuduConnectionProperties = new Properties();
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    InputStream kuduPropsFileAsStream = loader.getResourceAsStream(originalFileName);
    File tempPropertiesFile = null;
    try {
      tempPropertiesFile = File.createTempFile("kuduclusterconfig-" + System.currentTimeMillis(),
          ".properties");
      LOG.info(tempPropertiesFile.getAbsolutePath() + " is being used as a temporary props file");
    } catch (IOException e) {
      LOG.error("Not able to create temp file",e);
      return originalFileName;
    }
    if (kuduPropsFileAsStream != null) {
      try {
        kuduConnectionProperties.load(kuduPropsFileAsStream);
        kuduPropsFileAsStream.close();
        FileOutputStream toBeModifiedFileHandle = new FileOutputStream(tempPropertiesFile);
        kuduConnectionProperties.put(BaseKuduOutputOperator.MASTER_HOSTS,masterHosts);
        kuduConnectionProperties.store(toBeModifiedFileHandle,"updated by unit test");
        toBeModifiedFileHandle.close();
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      }
    }
    return tempPropertiesFile.getAbsolutePath();
  }

}
