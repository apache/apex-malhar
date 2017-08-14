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
package org.apache.apex.examples.twitter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.annotation.Nonnull;

import org.apache.apex.malhar.contrib.twitter.TwitterSampleInput;
import org.apache.apex.malhar.lib.db.jdbc.AbstractJdbcTransactionableOutputOperator;
import org.apache.hadoop.conf.Configuration;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import twitter4j.Status;

/**
 * An application which connects to Twitter Sample Input and stores all the
 * tweets with their usernames in a mysql database. Please review the docs
 * for TwitterTopCounterApplication to setup your twitter credentials. You
 * may also be able to change JDBCStore credentials using config file.
 *
 * You will also need to create appropriate database and tables with the
 * following schema, also included in mysql.sql in resources:
 * <pre>
 * DROP TABLE if exists tweets;
 * CREATE TABLE tweets (
 * window_id LONG NOT NULL,
 * creation_date DATE,
 * text VARCHAR(256) NOT NULL,
 * userid VARCHAR(40) NOT NULL,
 * KEY ( userid, creation_date)
 * );
 *
 * drop table if exists dt_window_id_tracker;
 * CREATE TABLE dt_window_id_tracker (
 * dt_application_id VARCHAR(100) NOT NULL,
 * dt_operator_id int(11) NOT NULL,
 * dt_window_id bigint NOT NULL,
 * UNIQUE (dt_application_id, dt_operator_id, dt_window_id)
 * )  ENGINE=MyISAM DEFAULT CHARSET=latin1;
 * </pre>
 *
 * @since 0.9.4
 */
@ApplicationAnnotation(name = "TwitterDumpExample")
public class TwitterDumpApplication implements StreamingApplication
{
  public static class Status2Database extends AbstractJdbcTransactionableOutputOperator<Status>
  {
    public static final String INSERT_STATUS_STATEMENT = "insert into tweets (window_id, creation_date, text, userid) values (?, ?, ?, ?)";

    public Status2Database()
    {
      store.setMetaTable("dt_window_id_tracker");
      store.setMetaTableAppIdColumn("dt_application_id");
      store.setMetaTableOperatorIdColumn("dt_operator_id");
      store.setMetaTableWindowColumn("dt_window_id");
    }

    @Nonnull
    @Override
    protected String getUpdateCommand()
    {
      return INSERT_STATUS_STATEMENT;
    }

    @Override
    protected void setStatementParameters(PreparedStatement statement, Status tuple) throws SQLException
    {
      statement.setLong(1, currentWindowId);

      statement.setDate(2, new java.sql.Date(tuple.getCreatedAt().getTime()));
      statement.setString(3, tuple.getText());
      statement.setString(4, tuple.getUser().getScreenName());
      statement.addBatch();
    }
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    //dag.setAttribute(DAGContext.APPLICATION_NAME, "TweetsDump");

    TwitterSampleInput twitterStream = dag.addOperator("TweetSampler", new TwitterSampleInput());

    //ConsoleOutputOperator dbWriter = dag.addOperator("DatabaseWriter", new ConsoleOutputOperator());

    Status2Database dbWriter = dag.addOperator("DatabaseWriter", new Status2Database());
    dbWriter.getStore().setDatabaseDriver("com.mysql.jdbc.Driver");
    dbWriter.getStore().setDatabaseUrl("jdbc:mysql://node6.morado.com:3306/twitter");
    dbWriter.getStore().setConnectionProperties("user:twitter");

    dag.addStream("Statuses", twitterStream.status, dbWriter.input).setLocality(Locality.CONTAINER_LOCAL);
  }

}
