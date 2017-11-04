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
package org.apache.apex.malhar.kudu;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.client.ExternalConsistencyMode;
import org.apache.kudu.client.SessionConfiguration;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <p>A Kudu input operator that provides the functionality of scanning a Kudu table by incrementing
 * a column in steps as specified in the configuration. Each iteration scans the table as given by the
 *  SQL expression in the properties file. Other aspects like connection details are also read from the properties file.
 * </p>
 *
 * <p>
 *   Here is an example property file format.
 *   Place a file named kuduincrementalstepscaninputoperator.properties anywhere in the classpath ( say by
 *    bundling in the resources section ). Alternately one can use the alternative constructor to specify the filename.
 *    The properties can be set as follows:
 * </p>
 *
 * <p>
 *   masterhosts=192.168.1.41:7051,192.168.1.54:7051
 *   tablename=kudutablename
 *templatequerystring=SELECT * KUDUTABLE WHERE COL1 > 1234 AND TIMESTAMPCOL >= :lowerbound AND TIMESTAMPCOL< :upperbound
 *   templatequerylowerboundparamname = :lowerbound
 *   templatequeryupperboundparamname = :upperbound
 *   templatequeryseedvalue = 1503001033219
 *   templatequeryincrementalvalue = 120000
 *
 *   The above property file will scan a table called KUDUTABLE and only scan those rows for which a column named COL1
 *   is greater than 1234. It also uses a column named TIMESTAMPCOL to start with the starting value of the lower
 *   bound as 1503001033219 and increments the scan window by 2 minutes. Note that :lowerbound and :upperbound are
 *   just template names that will be used for string substitution as the time windows progress.
 *
 *   Note that the implementation assumes the templated column data type is of type long. Considering other types
 *    is a trivial extenstion and perhaps best achieved by extending the Abstract implementation.
 * </p>
 *
 * @since 3.8.0
 */
public class IncrementalStepScanInputOperator<T,C extends InputOperatorControlTuple>
    extends AbstractKuduInputOperator<T,C>
{
  private static final Logger LOG = LoggerFactory.getLogger(IncrementalStepScanInputOperator.class);

  // The incremental value of the template column as read from the properties file
  private long stepUpValue;

  // The initial value of the templated column as read from the properties file
  private long startSeedValue;

  private long currentLowerBound;

  private long currentUpperBound;

  private String queryTemplateString;

  // The name of the parameter in the templated string that represents the lower bound
  private String lowerBoundParamName;

  // The name of the parameter in the templated string that represents the upperbound
  private String upperBoundParamName;

  public static String DEFAULT_CONNECTION_PROPS_FILE_NAME = "kuduincrementalstepscaninputoperator.properties";

  public static final String TABLE_NAME = "tablename";

  public static final String MASTER_HOSTS = "masterhosts";

  public static final String POJO_CLASS_NAME = "pojoclassname";

  // The name of the property in properties file that gives the query expression as a template
  public static final String TEMPLATE_QUERY_STRING = "templatequerystring";

  // The name of the property that specifies the lower bound name that will be used for incrementing
  public static final String TEMPLATE_QUERY_LOWERBOUND_PARAMNAME = "templatequerylowerboundparamname";

  // The name of the property that specifies the lower bound name that will be used for incrementing
  public static final String TEMPLATE_QUERY_UPPERBOUND_PARAMNAME = "templatequeryupperboundparamname";

  // The name of the property that specifies the value of the seed/starting value for the templated column
  public static final String TEMPLATE_QUERY_SEED_VALUE = "templatequeryseedvalue";

  // The name of the property that specifies the value of the increment for the templated column
  public static final String TEMPLATE_QUERY_INCREMENT_STEP_VALUE = "templatequeryincrementalvalue";

  private ApexKuduConnection.ApexKuduConnectionBuilder apexKuduConnectionBuilder;

  /***
   * To be used only by the Kryo deserialization logic and not encouraged for generic use.
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public IncrementalStepScanInputOperator() throws IOException, NumberFormatException
  {
    initConnectionBuilderProperties(DEFAULT_CONNECTION_PROPS_FILE_NAME);
  }

  public IncrementalStepScanInputOperator(Class<T> classForPojo, String configFileInClasspath)
    throws IOException, NumberFormatException
  {
    initConnectionBuilderProperties(configFileInClasspath);
    clazzForResultObject = classForPojo;
  }

  /***
   * Instantiates all of the common configurations. Also does the initiation logic for the mandatory fields in super
   * class
   * @param configFileInClasspath The properties file name
   * @throws IOException If not able to read properties file
   * @throws NumberFormatException If not able to format string as long
   */
  private void initConnectionBuilderProperties(String configFileInClasspath)
    throws IOException, NumberFormatException
  {
    Properties incrementalStepProperties = new Properties();
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    InputStream kuduPropsFileAsStream = loader.getResourceAsStream(configFileInClasspath);
    if (kuduPropsFileAsStream != null) {
      incrementalStepProperties.load(kuduPropsFileAsStream);
    } else {
      if (!DEFAULT_CONNECTION_PROPS_FILE_NAME.equalsIgnoreCase(configFileInClasspath)) {
        throw new IOException("Properties file required for Kudu connection " + configFileInClasspath +
          " is not locatable in the root classpath");
      } else {
        LOG.warn("Properties file could not be loaded. Expecting the user to set properties manually");
      }
    }
    tableName = checkNotNull(incrementalStepProperties.getProperty(TABLE_NAME));
    String masterHostsConnectionString = checkNotNull(incrementalStepProperties.getProperty(MASTER_HOSTS));
    String[] masterAndHosts = masterHostsConnectionString.split(",");
    lowerBoundParamName = checkNotNull(incrementalStepProperties.getProperty(
        TEMPLATE_QUERY_LOWERBOUND_PARAMNAME));
    upperBoundParamName = checkNotNull(incrementalStepProperties.getProperty(
        TEMPLATE_QUERY_UPPERBOUND_PARAMNAME));
    String stepUpValueString = checkNotNull(incrementalStepProperties.getProperty(TEMPLATE_QUERY_INCREMENT_STEP_VALUE));
    String seedValueString =  checkNotNull(incrementalStepProperties.getProperty(TEMPLATE_QUERY_SEED_VALUE));
    queryTemplateString = checkNotNull(incrementalStepProperties.getProperty(TEMPLATE_QUERY_STRING));
    startSeedValue = Long.valueOf(seedValueString);
    stepUpValue = Long.valueOf(stepUpValueString);
    currentLowerBound = startSeedValue;
    currentUpperBound = currentLowerBound + startSeedValue;
    initKuduConfig(tableName, Arrays.asList(masterAndHosts));
  }

  /***
   * A simple init of the kudu connection config. Override this if you would like to fine tune hte connection parameters
   * @param kuduTableName The Kudu table name
   * @param kuduMasters The master hosts of the Kudu cluster.
   */
  public void initKuduConfig(String kuduTableName, List<String> kuduMasters)
  {
    apexKuduConnectionBuilder = new ApexKuduConnection.ApexKuduConnectionBuilder()
      .withTableName(kuduTableName)
      .withExternalConsistencyMode(ExternalConsistencyMode.COMMIT_WAIT)
      .withFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC)
      .withNumberOfBossThreads(1)
      .withNumberOfWorkerThreads(2)
      .withSocketReadTimeOutAs(3000)
      .withOperationTimeOutAs(3000);
    for ( String aMasterAndHost: kuduMasters ) {
      apexKuduConnectionBuilder = apexKuduConnectionBuilder.withAPossibleMasterHostAs(aMasterAndHost);
    }
    apexKuduConnectionInfo = apexKuduConnectionBuilder;
  }

  /***
   * A simple template replacement logic which provides the next window of the query by replacing the
   * lower and upper bound parameters accordingly.
   * @return Returns the next in line query as a String that is compliant with the Kudu SQL grammar.
   */
  @Override
  protected String getNextQuery()
  {
    long lowerBoundToUseForNextIteration = currentLowerBound;
    long upperBoundToUseForNextIteration = currentUpperBound;
    String queryToUse = new String(queryTemplateString);
    queryToUse = queryToUse.replace(lowerBoundParamName,("" + lowerBoundToUseForNextIteration));
    queryToUse = queryToUse.replace(upperBoundParamName,("" + upperBoundToUseForNextIteration));
    currentLowerBound = currentUpperBound;
    currentUpperBound = currentLowerBound + stepUpValue;
    return queryToUse;
  }

  public long getStepUpValue()
  {
    return stepUpValue;
  }

  public void setStepUpValue(long stepUpValue)
  {
    this.stepUpValue = stepUpValue;
  }

  public long getStartSeedValue()
  {
    return startSeedValue;
  }

  public void setStartSeedValue(long startSeedValue)
  {
    this.startSeedValue = startSeedValue;
    currentLowerBound = startSeedValue;
    currentUpperBound = currentLowerBound + startSeedValue;
  }

  public String getQueryTemplateString()
  {
    return queryTemplateString;
  }

  public void setQueryTemplateString(String queryTemplateString)
  {
    this.queryTemplateString = queryTemplateString;
  }

  public String getLowerBoundParamName()
  {
    return lowerBoundParamName;
  }

  public void setLowerBoundParamName(String lowerBoundParamName)
  {
    this.lowerBoundParamName = lowerBoundParamName;
  }

  public String getUpperBoundParamName()
  {
    return upperBoundParamName;
  }

  public void setUpperBoundParamName(String upperBoundParamName)
  {
    this.upperBoundParamName = upperBoundParamName;
  }
}
