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
package org.apache.apex.malhar.contrib.cassandra;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

import org.apache.apex.malhar.lib.helper.TestPortContext;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AbstractUpsertOutputOperatorCodecsTest
{

  /**
   * The schema that is used
   *
   *
   *
   *
   * CREATE KEYSPACE unittests
   * WITH replication = {
   * 'class' : 'SimpleStrategy',
   * 'replication_factor' : 1
   * };
   *
   * CREATE TYPE unittests.address (
   * street text,
   * city text,
   * zip_code int,
   * phones set<text>
   * );
   *
   * CREATE TYPE unittests.fullname ( firstname text, lastname text );
   *
   * CREATE TABLE unittests.users (
   * userid text PRIMARY KEY,
   * username FROZEN<fullname>,
   * emails set<text>,
   * top_scores list<int>,
   * todo map<timestamp, text>,
   * siblings tuple<int, text,text>,
   * currentaddress FROZEN<address>,
   * previousnames FROZEN<list<fullname>>
   * );
   *
   * CREATE TABLE unittests.userupdates (
   * userid text PRIMARY KEY,
   * updatecount counter
   * );
   *
   * CREATE TABLE unittests.userstatus (
   * userid text,
   * day int,
   * month int,
   * year int,
   * employeeid text,
   * currentstatus text,
   * PRIMARY KEY ((userid,day,month,year), employeeid));
   */

  public static final String APP_ID = "TestCassandraUpsertOperator";
  public static final int OPERATOR_ID_FOR_USER_UPSERTS = 0;


  UserUpsertOperator userUpsertOperator = null;
  OperatorContext contextForUserUpsertOperator;
  TestPortContext testPortContextForUserUpserts;


  @Before
  public void setupApexContexts() throws Exception
  {
    Attribute.AttributeMap.DefaultAttributeMap attributeMap = new Attribute.AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    contextForUserUpsertOperator = mockOperatorContext(OPERATOR_ID_FOR_USER_UPSERTS, attributeMap);
    userUpsertOperator = new UserUpsertOperator();

    Attribute.AttributeMap.DefaultAttributeMap portAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    portAttributes.put(Context.PortContext.TUPLE_CLASS, User.class);
    testPortContextForUserUpserts = new TestPortContext(portAttributes);

    userUpsertOperator.setup(contextForUserUpsertOperator);
    userUpsertOperator.activate(contextForUserUpsertOperator);
    userUpsertOperator.input.setup(testPortContextForUserUpserts);
  }

  @Test
  public void testActivateForSchemaDetection() throws Exception
  {
    assertEquals(12, userUpsertOperator.getPreparedStatementTypes().size());
    assertEquals(
        " UPDATE unittests.users  SET  currentaddress = :currentaddress, emails = emails - :emails, " +
        "todo = todo - :todo, top_scores = top_scores - :top_scores, siblings = :siblings, " +
        "previousnames = :previousnames, username = :username WHERE  userid = :userid",
        userUpsertOperator.getPreparedStatementTypes().get(101001100L).getQueryString());
    assertEquals(8, userUpsertOperator.getColumnDefinitions().size());
    assertEquals(true, userUpsertOperator.getColumnDefinitions().get("currentaddress").isFrozen());
    assertEquals(false, userUpsertOperator.getColumnDefinitions().get("currentaddress").isCollection());
    assertEquals(true, userUpsertOperator.getColumnDefinitions().get("top_scores").isCollection());
    assertEquals(true, userUpsertOperator.getColumnDefinitions().get("username").isFrozen());
    assertEquals(false, userUpsertOperator.getColumnDefinitions().get("username").isCollection());
  }

  @Test
  public void testForGetters() throws Exception
  {
    Map<String, Object> getters = userUpsertOperator.getGetters();
    assertNotNull(getters);
    assertEquals(7, getters.size());
  }

  @Test
  public void testForSingleRowInsertWithCodecs() throws Exception
  {
    User aUser = new User();
    aUser.setUserid("user" + System.currentTimeMillis());
    FullName fullName = new FullName("first1" + System.currentTimeMillis(), "last1" + System.currentTimeMillis());
    aUser.setUsername(fullName);
    Address address = new Address("wer", "hjfh", 12, null);
    aUser.setCurrentaddress(address);
    UpsertExecutionContext<User> anUpdate = new UpsertExecutionContext<>();
    anUpdate.setPayload(aUser);
    userUpsertOperator.beginWindow(0);
    userUpsertOperator.input.process(anUpdate);
    userUpsertOperator.endWindow();

    ResultSet results = userUpsertOperator.session.execute(
        "SELECT * FROM unittests.users WHERE userid = '" + aUser.getUserid() + "'");
    List<Row> rows = results.all();
    assertEquals(rows.size(), 1);
    assertTrue(results.isExhausted());
  }

  @Test
  public void testForListAppend() throws Exception
  {
    User aUser = new User();
    String userId = "user" + System.currentTimeMillis();
    aUser.setUserid(userId);
    FullName fullName = new FullName("first1" + System.currentTimeMillis(), "last1" + System.currentTimeMillis());
    aUser.setUsername(fullName);
    Address address = new Address("street1", "city1", 13, null);
    aUser.setCurrentaddress(address);
    Set<String> emails = new HashSet<>();
    emails.add(new String("1"));
    emails.add(new String("2"));
    aUser.setEmails(emails);
    List<Integer> topScores = new ArrayList<>();
    topScores.add(1);
    topScores.add(2);
    aUser.setTopScores(topScores);
    UpsertExecutionContext<User> originalEntry = new UpsertExecutionContext<>();
    originalEntry.setPayload(aUser);

    UpsertExecutionContext<User> subsequentUpdateForTopScores = new UpsertExecutionContext<>();
    subsequentUpdateForTopScores.setListPlacementStyle(
        UpsertExecutionContext.ListPlacementStyle.APPEND_TO_EXISTING_LIST);
    subsequentUpdateForTopScores.setCollectionMutationStyle(
        UpsertExecutionContext.CollectionMutationStyle.ADD_TO_EXISTING_COLLECTION);
    subsequentUpdateForTopScores.setNullHandlingMutationStyle(
        UpsertExecutionContext.NullHandlingMutationStyle.IGNORE_NULL_COLUMNS);
    User oldUser = new User();
    oldUser.setUserid(userId);
    List<Integer> topScoresAppended = new ArrayList<>();
    topScoresAppended.add(3);
    oldUser.setTopScores(topScoresAppended);
    subsequentUpdateForTopScores.setPayload(oldUser);
    userUpsertOperator.beginWindow(1);
    userUpsertOperator.input.process(originalEntry);
    userUpsertOperator.input.process(subsequentUpdateForTopScores);
    userUpsertOperator.endWindow();

    ResultSet results = userUpsertOperator.session.execute(
        "SELECT * FROM unittests.users WHERE userid = '" + userId + "'");
    List<Row> rows = results.all();
    Row userRow = rows.get(0);
    List<Integer> topScoresEntry = userRow.getList("top_scores", Integer.class);
    assertEquals(3, topScoresEntry.size());
    assertEquals("" + 3, "" + topScoresEntry.get(2));
  }

  @Test
  public void testForListPrepend() throws Exception
  {
    User aUser = new User();
    String userId = "user" + System.currentTimeMillis();
    aUser.setUserid(userId);
    FullName fullName = new FullName("first1" + System.currentTimeMillis(), "last1" + System.currentTimeMillis());
    aUser.setUsername(fullName);
    List<Integer> topScores = new ArrayList<>();
    topScores.add(1);
    topScores.add(2);
    aUser.setTopScores(topScores);
    UpsertExecutionContext<User> originalEntry = new UpsertExecutionContext<>();
    originalEntry.setPayload(aUser);

    UpsertExecutionContext<User> subsequentUpdateForTopScores = new UpsertExecutionContext<>();
    subsequentUpdateForTopScores.setListPlacementStyle(
        UpsertExecutionContext.ListPlacementStyle.PREPEND_TO_EXISTING_LIST);
    subsequentUpdateForTopScores.setCollectionMutationStyle(
        UpsertExecutionContext.CollectionMutationStyle.ADD_TO_EXISTING_COLLECTION);
    subsequentUpdateForTopScores.setNullHandlingMutationStyle(
        UpsertExecutionContext.NullHandlingMutationStyle.IGNORE_NULL_COLUMNS);
    User oldUser = new User();
    oldUser.setUserid(userId);
    List<Integer> topScoresAppended = new ArrayList<>();
    topScoresAppended.add(3);
    oldUser.setTopScores(topScoresAppended);
    subsequentUpdateForTopScores.setPayload(oldUser);
    userUpsertOperator.beginWindow(2);
    userUpsertOperator.input.process(originalEntry);
    userUpsertOperator.input.process(subsequentUpdateForTopScores);
    userUpsertOperator.endWindow();

    ResultSet results = userUpsertOperator.session.execute(
        "SELECT * FROM unittests.users WHERE userid = '" + userId + "'");
    List<Row> rows = results.all();
    Row userRow = rows.get(0);
    List<Integer> topScoresEntry = userRow.getList("top_scores", Integer.class);
    assertEquals(3, topScoresEntry.size());
    assertEquals("" + 3, "" + topScoresEntry.get(0));
  }

  @Test
  public void testForCollectionRemoval() throws Exception
  {
    User aUser = new User();
    String userId = "user" + System.currentTimeMillis();
    aUser.setUserid(userId);
    FullName fullName = new FullName("first12" + System.currentTimeMillis(), "last12" + System.currentTimeMillis());
    aUser.setUsername(fullName);
    Set<String> emails = new HashSet<>();
    emails.add(new String("1"));
    emails.add(new String("2"));
    aUser.setEmails(emails);

    UpsertExecutionContext<User> originalEntry = new UpsertExecutionContext<>();
    originalEntry.setPayload(aUser);

    UpsertExecutionContext<User> subsequentUpdateForEmails = new UpsertExecutionContext<>();
    subsequentUpdateForEmails.setCollectionMutationStyle(
        UpsertExecutionContext.CollectionMutationStyle.REMOVE_FROM_EXISTING_COLLECTION);
    subsequentUpdateForEmails.setNullHandlingMutationStyle(
        UpsertExecutionContext.NullHandlingMutationStyle.IGNORE_NULL_COLUMNS);
    User oldUser = new User();
    oldUser.setUserid(userId);
    Set<String> updatedEmails = new HashSet<>();
    updatedEmails.add(new String("1"));
    oldUser.setEmails(updatedEmails);
    subsequentUpdateForEmails.setPayload(oldUser);
    userUpsertOperator.beginWindow(3);
    userUpsertOperator.input.process(originalEntry);
    userUpsertOperator.input.process(subsequentUpdateForEmails);
    userUpsertOperator.endWindow();

    ResultSet results = userUpsertOperator.session.execute(
        "SELECT * FROM unittests.users WHERE userid = '" + userId + "'");
    List<Row> rows = results.all();
    Row userRow = rows.get(0);
    Set<String> existingEmailsEntry = userRow.getSet("emails", String.class);
    assertEquals(1, existingEmailsEntry.size());
    assertEquals("" + 2, "" + existingEmailsEntry.iterator().next());
  }

  @Test
  public void testForCollectionRemovalAndIfExists() throws Exception
  {
    User aUser = new User();
    String userId = "user" + System.currentTimeMillis();
    aUser.setUserid(userId);
    FullName fullName = new FullName("first12" + System.currentTimeMillis(), "last12" + System.currentTimeMillis());
    aUser.setUsername(fullName);
    Set<String> emails = new HashSet<>();
    emails.add(new String("1"));
    emails.add(new String("2"));
    aUser.setEmails(emails);

    UpsertExecutionContext<User> originalEntry = new UpsertExecutionContext<>();
    originalEntry.setPayload(aUser);

    UpsertExecutionContext<User> subsequentUpdateForEmails = new UpsertExecutionContext<>();
    subsequentUpdateForEmails.setCollectionMutationStyle(
        UpsertExecutionContext.CollectionMutationStyle.REMOVE_FROM_EXISTING_COLLECTION);
    subsequentUpdateForEmails.setNullHandlingMutationStyle(
        UpsertExecutionContext.NullHandlingMutationStyle.IGNORE_NULL_COLUMNS);
    subsequentUpdateForEmails.setUpdateOnlyIfPrimaryKeyExists(true);
    User oldUser = new User();
    oldUser.setUserid(userId + System.currentTimeMillis()); // overriding with a non-existent user id
    Set<String> updatedEmails = new HashSet<>();
    updatedEmails.add(new String("1"));
    oldUser.setEmails(updatedEmails);
    subsequentUpdateForEmails.setPayload(oldUser);
    userUpsertOperator.beginWindow(4);
    userUpsertOperator.input.process(originalEntry);
    userUpsertOperator.input.process(subsequentUpdateForEmails);
    userUpsertOperator.endWindow();

    ResultSet results = userUpsertOperator.session.execute(
        "SELECT * FROM unittests.users WHERE userid = '" + userId + "'");
    List<Row> rows = results.all();
    Row userRow = rows.get(0);
    Set<String> existingEmailsEntry = userRow.getSet("emails", String.class);
    assertEquals(2, existingEmailsEntry.size());
    assertEquals("" + 1, "" + existingEmailsEntry.iterator().next());
  }

  @Test
  public void testForListAppendAndIfExists() throws Exception
  {
    User aUser = new User();
    String userId = "user" + System.currentTimeMillis();
    aUser.setUserid(userId);
    FullName fullName = new FullName("first" + System.currentTimeMillis(), "last" + System.currentTimeMillis());
    aUser.setUsername(fullName);
    Address address = new Address("street1", "city1", 13, null);
    aUser.setCurrentaddress(address);
    Set<String> emails = new HashSet<>();
    emails.add(new String("1"));
    emails.add(new String("2"));
    aUser.setEmails(emails);
    List<Integer> topScores = new ArrayList<>();
    topScores.add(1);
    topScores.add(2);
    aUser.setTopScores(topScores);
    UpsertExecutionContext<User> originalEntry = new UpsertExecutionContext<>();
    originalEntry.setPayload(aUser);

    UpsertExecutionContext<User> subsequentUpdateForTopScores = new UpsertExecutionContext<>();
    subsequentUpdateForTopScores.setListPlacementStyle(
        UpsertExecutionContext.ListPlacementStyle.APPEND_TO_EXISTING_LIST);
    subsequentUpdateForTopScores.setCollectionMutationStyle(
        UpsertExecutionContext.CollectionMutationStyle.ADD_TO_EXISTING_COLLECTION);
    subsequentUpdateForTopScores.setNullHandlingMutationStyle(
        UpsertExecutionContext.NullHandlingMutationStyle.IGNORE_NULL_COLUMNS);
    subsequentUpdateForTopScores.setUpdateOnlyIfPrimaryKeyExists(true);
    User oldUser = new User();
    oldUser.setUserid(userId + System.currentTimeMillis());
    List<Integer> topScoresAppended = new ArrayList<>();
    topScoresAppended.add(3);
    oldUser.setTopScores(topScoresAppended);
    subsequentUpdateForTopScores.setPayload(oldUser);
    userUpsertOperator.beginWindow(5);
    userUpsertOperator.input.process(originalEntry);
    userUpsertOperator.input.process(subsequentUpdateForTopScores);
    userUpsertOperator.endWindow();

    ResultSet results = userUpsertOperator.session.execute(
        "SELECT * FROM unittests.users WHERE userid = '" + userId + "'");
    List<Row> rows = results.all();
    Row userRow = rows.get(0);
    List<Integer> topScoresEntry = userRow.getList("top_scores", Integer.class);
    assertEquals(2, topScoresEntry.size());
    assertEquals("" + 2, "" + topScoresEntry.get(1));
  }

  @Test
  public void testForListPrependAndExplicitNullForSomeColumns() throws Exception
  {
    User aUser = new User();
    String userId = "user" + System.currentTimeMillis();
    aUser.setUserid(userId);
    FullName fullName = new FullName("first24" + System.currentTimeMillis(), "last" + System.currentTimeMillis());
    aUser.setUsername(fullName);
    List<Integer> topScores = new ArrayList<>();
    topScores.add(1);
    topScores.add(2);
    aUser.setTopScores(topScores);
    UpsertExecutionContext<User> originalEntry = new UpsertExecutionContext<>();
    originalEntry.setPayload(aUser);

    UpsertExecutionContext<User> subsequentUpdateForTopScores = new UpsertExecutionContext<>();
    subsequentUpdateForTopScores.setListPlacementStyle(
        UpsertExecutionContext.ListPlacementStyle.PREPEND_TO_EXISTING_LIST);
    subsequentUpdateForTopScores.setCollectionMutationStyle(
        UpsertExecutionContext.CollectionMutationStyle.ADD_TO_EXISTING_COLLECTION);
    subsequentUpdateForTopScores.setNullHandlingMutationStyle(
        UpsertExecutionContext.NullHandlingMutationStyle.SET_NULL_COLUMNS);
    User oldUser = new User();
    oldUser.setUserid(userId);
    List<Integer> topScoresAppended = new ArrayList<>();
    topScoresAppended.add(3);
    oldUser.setTopScores(topScoresAppended);
    subsequentUpdateForTopScores.setPayload(oldUser);
    userUpsertOperator.beginWindow(6);
    userUpsertOperator.input.process(originalEntry);
    userUpsertOperator.input.process(subsequentUpdateForTopScores);
    userUpsertOperator.endWindow();

    ResultSet results = userUpsertOperator.session.execute(
        "SELECT * FROM unittests.users WHERE userid = '" + userId + "'");
    List<Row> rows = results.all();
    Row userRow = rows.get(0);
    FullName name = userRow.get("username", FullName.class);
    assertEquals(null, name);
  }

  @Test
  public void testForSingleRowInsertWithTTL() throws Exception
  {
    User aUser = new User();
    aUser.setUserid("userWithTTL" + System.currentTimeMillis());
    FullName fullName = new FullName("firstname" + System.currentTimeMillis(), "lasName" + System.currentTimeMillis());
    aUser.setUsername(fullName);
    Address address = new Address("city1", "Street1", 12, null);
    aUser.setCurrentaddress(address);
    UpsertExecutionContext<User> anUpdate = new UpsertExecutionContext<>();
    anUpdate.setOverridingTTL(5000);
    anUpdate.setPayload(aUser);
    userUpsertOperator.beginWindow(7);
    userUpsertOperator.input.process(anUpdate);
    userUpsertOperator.endWindow();

    ResultSet results = userUpsertOperator.session.execute(
        "SELECT * FROM unittests.users WHERE userid = '" + aUser.getUserid() + "'");
    List<Row> rows = results.all();
    assertEquals(rows.size(), 1);
    assertTrue(results.isExhausted());
  }

  @Test
  public void testForSingleRowInsertWithOverridingConsistency() throws Exception
  {
    User aUser = new User();
    aUser.setUserid("userWithConsistency" + System.currentTimeMillis());
    FullName fullName = new FullName("first" + System.currentTimeMillis(), "last" + System.currentTimeMillis());
    aUser.setUsername(fullName);
    Address address = new Address("city21", "Street31", 12, null);
    aUser.setCurrentaddress(address);
    UpsertExecutionContext<User> anUpdate = new UpsertExecutionContext<>();
    anUpdate.setOverridingConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);
    anUpdate.setPayload(aUser);
    userUpsertOperator.beginWindow(8);
    userUpsertOperator.input.process(anUpdate);
    userUpsertOperator.endWindow();

    ResultSet results = userUpsertOperator.session.execute(
        "SELECT * FROM unittests.users WHERE userid = '" + aUser.getUserid() + "'");
    List<Row> rows = results.all();
    assertEquals(rows.size(), 1);
    assertTrue(results.isExhausted());
  }


}
