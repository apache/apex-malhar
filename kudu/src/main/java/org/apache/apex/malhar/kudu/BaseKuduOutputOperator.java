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

import org.apache.apex.malhar.lib.wal.FSWindowDataManager;
import org.apache.kudu.client.ExternalConsistencyMode;
import org.apache.kudu.client.SessionConfiguration;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Provides a default implementation for writing tuples as Kudu rows.
 * The user will have to either provide for
 * <ol>
 * <li>a property file containing properties like Kudu master list, Kudu table name and other connection properties.
 * The operator will fail to launch if the properties file named kuduoutputoperator.properties is not locatable in the
 *  root path</li>
 * <li>Use the default constructor which supports minimum required properties as parameters</li>
 * <li>In case of presence of multiple Kudu output operators in the same application, use the String based
 * constructor which accepts a file name for each of the kudu table that the incoming pojo needs to be passivated
 *  to</li>
 * </ol>
 * <p>
 * The properties file will have to consist of the following keys:
 * <br>masterhosts=<ip1:host>,<ip2:host>,..# Comma separated</br>
 * <br>tablename=akudutablename</br>
 * <br>pojoclassname=somepojoclasswithgettersandsetters; # Do not append name with .class at the end and
 *  do not forget to give the complete class name including the package</br>
 * </p>
 *
 * @since 3.8.0
 */
public class BaseKuduOutputOperator extends AbstractKuduOutputOperator
{
  public static String DEFAULT_CONNECTION_PROPS_FILE_NAME = "kuduoutputoperator.properties";

  public static final String TABLE_NAME = "tablename";

  public static final String MASTER_HOSTS = "masterhosts";

  public static final String POJO_CLASS_NAME = "pojoclassname";

  private Class pojoPayloadClass;

  private ApexKuduConnection.ApexKuduConnectionBuilder apexKuduConnectionBuilder;

  public BaseKuduOutputOperator() throws IOException, ClassNotFoundException
  {
    windowDataManager = new FSWindowDataManager();
    initConnectionBuilderProperties(DEFAULT_CONNECTION_PROPS_FILE_NAME);
  }

  public BaseKuduOutputOperator(String configFileInClasspath) throws IOException, ClassNotFoundException
  {
    windowDataManager = new FSWindowDataManager();
    initConnectionBuilderProperties(configFileInClasspath);
  }

  private void initConnectionBuilderProperties(String configFileInClasspath) throws IOException, ClassNotFoundException
  {
    Properties kuduConnectionProperties = new Properties();
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    InputStream kuduPropsFileAsStream = loader.getResourceAsStream(configFileInClasspath);
    if (kuduPropsFileAsStream != null) {
      kuduConnectionProperties.load(kuduPropsFileAsStream);
      kuduPropsFileAsStream.close();
    } else {
      throw new IOException("Properties file required for Kudu connection " + configFileInClasspath +
      " is not locatable in the root classpath");
    }
    String tableName = checkNotNull(kuduConnectionProperties.getProperty(TABLE_NAME));
    String pojoClassName = checkNotNull(kuduConnectionProperties.getProperty(POJO_CLASS_NAME));
    String masterHostsConnectionString = checkNotNull(kuduConnectionProperties.getProperty(MASTER_HOSTS));
    String[] masterAndHosts = masterHostsConnectionString.split(",");
    pojoPayloadClass = Class.forName(pojoClassName);
    initKuduConfig(tableName, Arrays.asList(masterAndHosts));
  }

  private void initKuduConfig(String kuduTableName, List<String> kuduMasters)
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
  }


  public BaseKuduOutputOperator(String kuduTableName,List<String> kuduMasters, Class pojoPayloadClass)
  {
    this.pojoPayloadClass = pojoPayloadClass;
    initKuduConfig(kuduTableName,kuduMasters);
  }

  @Override
  ApexKuduConnection.ApexKuduConnectionBuilder getKuduConnectionConfig()
  {
    return apexKuduConnectionBuilder;
  }

  /**
   * Can be used to further fine tune any of the connection configs once the constructor completes instantiating the
   * Kudu Connection config builder.
   * @return The Connection Config that would be used to initiate a connection to the Kudu Cluster once the operator is
   * deserialized in the node manager managed container. See {@link AbstractKuduOutputOperator} activate() method for
   * more details.
   */
  public ApexKuduConnection.ApexKuduConnectionBuilder getApexKuduConnectionBuilder()
  {
    return apexKuduConnectionBuilder;
  }


  /**
   *
   * @return The pojo class that would be streamed in the KuduExecutionContext
   */
  @Override
  protected Class getTuplePayloadClass()
  {
    return pojoPayloadClass;
  }

  /**
   * The default is to implement for ATLEAST_ONCE semantics. Override this control this behavior.
   * @param executionContext The tuple which represents the execution context along with the payload
   * @param reconcilingWindowId The window Id of the reconciling window
   * @return True if the current row is to be written to the Kudu Store, false to be skipped
   */
  @Override
  protected boolean isEligibleForPassivationInReconcilingWindow(KuduExecutionContext executionContext,
      long reconcilingWindowId)
  {
    return true;
  }
}
