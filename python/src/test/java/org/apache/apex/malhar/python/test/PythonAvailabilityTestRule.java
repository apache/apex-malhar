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
package org.apache.apex.malhar.python.test;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.base.jep.BaseJEPTest;

/**
 * A Junit rule that helps in bypassing tests that cannot be done if the Python installation is not present.
 * The unit tests will be triggered as soon as the switch -DjepInstalled=true is passed from the command line.
 */
public class PythonAvailabilityTestRule implements TestRule
{

  private static final transient Logger LOG = LoggerFactory.getLogger(PythonAvailabilityTestRule.class);

  public static final String JEP_LIBRARY_INSTALLED_SWITCH = "jepInstallPath";

  @Override
  public Statement apply(Statement base, Description description)
  {
    JepPythonTestContext testContext = description.getAnnotation(JepPythonTestContext.class);
    String jepInstalledStrVal = System.getProperty(JEP_LIBRARY_INSTALLED_SWITCH);
    boolean jepInstalled = false;
    if (jepInstalledStrVal != null) {
      jepInstalled = true;
      LOG.debug("Using " + jepInstalledStrVal + " as the library path for python interpreter");
    }
    boolean runThisTest = true; // default is to run the test if no annotation is specified i.e. python is not required
    if ( testContext != null) {
      if ( (testContext.jepPythonBasedTest()) && (jepInstalled) ) {
        runThisTest = true;
      } else {
        runThisTest = false;
      }
    }
    if (runThisTest) {
      if ( (jepInstalled) && (!BaseJEPTest.JEP_INITIALIZED)) {
        try {
          BaseJEPTest.initJEPThread();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
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

}
