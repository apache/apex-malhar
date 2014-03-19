/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.twitter;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import twitter4j.Status;

import com.datatorrent.contrib.jdbc.JDBCOperatorBase;
import com.datatorrent.contrib.twitter.TwitterSampleInput;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DAGContext;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.annotation.ShipContainingJars;

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
 */
@ApplicationAnnotation(name="TwitterTopCounterApplicationWithDB")
public class TwitterDumpApplication implements StreamingApplication
{
  @ShipContainingJars(classes = {com.mysql.jdbc.Driver.class})
  public static class Status2Database extends JDBCOutputOperator<Status>
  {
    public static final String INSERT_STATUS_STATEMENT = "insert into tweets (window_id, creation_date, text, userid) values (?, ?, ?, ?)";
    public static final String SELECT_WINDOW_ID_STATEMENT = "select dt_window_id from dt_window_id_tracker where dt_application_id = ? and dt_operator_id = ?";
    public static final String INSERT_WINDOW_ID_STATEMENT = "insert into dt_window_id_tracker (dt_application_id, dt_operator_id, dt_window_id) values (?, ?, ?)";
    public static final String UPDATE_WINDOW_ID_STATEMENT = "update dt_window_id_tracker set dt_window_id = ? where dt_application_id = ? and dt_operator_id = ?";
    private transient CallableStatement insertTuple;
    private transient PreparedStatement lastWindowUpdateStmt;

    @Override
    protected PreparedStatement getBatchUpdateCommandFor(List<Status> tuples, long windowId) throws SQLException
    {
      insertTuple.setLong(1, windowId);

      for (Status status : tuples) {
        insertTuple.setDate(2, new java.sql.Date(status.getCreatedAt().getTime()));
        insertTuple.setString(3, status.getText());
        insertTuple.setString(4, status.getUser().getScreenName());
        insertTuple.addBatch();
      }

      return insertTuple;
    }

    @Override
    protected long getLastPersistedWindow(OperatorContext context) throws Exception
    {
      PreparedStatement stmt = jdbcConnector.getConnection().prepareStatement(SELECT_WINDOW_ID_STATEMENT);
      stmt.setString(1, context.getValue(DAG.APPLICATION_ID));
      stmt.setInt(2, context.getId());
      long lastWindow = -1;
      ResultSet resultSet = stmt.executeQuery();
      if (resultSet.next()) {
        lastWindow = resultSet.getLong(1);
      }
      else {
        stmt = jdbcConnector.getConnection().prepareStatement(INSERT_WINDOW_ID_STATEMENT);
        stmt.setString(1, context.getValue(DAG.APPLICATION_ID));
        stmt.setInt(2, context.getId());
        stmt.setLong(3, -1);
        stmt.executeUpdate();
      }
      stmt.close();
      return lastWindow;
    }

    @Override
    protected void updateLastCommittedWindow(long window) throws Exception
    {
      lastWindowUpdateStmt.setLong(1, window);
      lastWindowUpdateStmt.executeUpdate();
    }

    @Override
    public void setup(OperatorContext context)
    {
      super.setup(context);
      try {
        insertTuple = jdbcConnector.getConnection().prepareCall(INSERT_STATUS_STATEMENT);

        lastWindowUpdateStmt = jdbcConnector.getConnection().prepareStatement(UPDATE_WINDOW_ID_STATEMENT);
        lastWindowUpdateStmt.setString(2, context.getValue(DAG.APPLICATION_ID));
        lastWindowUpdateStmt.setInt(3, context.getId());
      }
      catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAGContext.APPLICATION_NAME, "TweetsDump");

    TwitterSampleInput twitterStream = dag.addOperator("TweetSampler", new TwitterSampleInput());

    //ConsoleOutputOperator dbWriter = dag.addOperator("DatabaseWriter", new ConsoleOutputOperator());
    JDBCOperatorBase jdbcStore = new JDBCOperatorBase();
    jdbcStore.setDbDriver("com.mysql.jdbc.Driver");
    jdbcStore.setDbUrl("jdbc:mysql://node6.morado.com:3306/twitter");
    jdbcStore.setUserName("twitter");

    Status2Database dbWriter = dag.addOperator("DatabaseWriter", new Status2Database());
    dbWriter.setJdbcStore(jdbcStore);

    dag.addStream("Statuses", twitterStream.status, dbWriter.input).setLocality(Locality.CONTAINER_LOCAL);
  }

}
