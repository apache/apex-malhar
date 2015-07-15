package com.datatorrent.contrib.enrichment;

import com.datatorrent.netlet.util.DTThrowable;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.slf4j.LoggerFactory;

public class JDBCLoaderTest
{
  static final org.slf4j.Logger logger = LoggerFactory.getLogger(JDBCLoaderTest.class);

  public static class TestMeta extends TestWatcher
  {
    JDBCLoader dbloader;
    @Override
    protected void starting(Description description)
    {
        try {
          dbloader = new JDBCLoader();
          dbloader.setDatabaseDriver("org.hsqldb.jdbcDriver");
          dbloader.setDatabaseUrl("jdbc:hsqldb:mem:test;sql.syntax_mys=true");
          dbloader.setTableName("COMPANY");

          dbloader.connect();
          createTable();
          insertRecordsInTable();
        }
        catch (Throwable e) {
            DTThrowable.rethrow(e);
        }
    }

    private void createTable()
    {
        try {
            Statement stmt = dbloader.getConnection().createStatement();

            String createTable = "CREATE TABLE IF NOT EXISTS " + dbloader.getTableName() +
                    "(ID INT PRIMARY KEY     NOT NULL," +
                    " NAME           TEXT    NOT NULL, " +
                    " AGE            INT     NOT NULL, " +
                    " ADDRESS        CHAR(50), " +
                    " SALARY         REAL)";
            stmt.executeUpdate(createTable);

            logger.debug("Table  created successfully...");
        }
        catch (Throwable e) {
            DTThrowable.rethrow(e);
        }
    }

    private void insertRecordsInTable()
    {
      try {
        Statement stmt = dbloader.getConnection().createStatement();
        String tbName = dbloader.getTableName();

        String sql = "INSERT INTO " + tbName + " (ID,NAME,AGE,ADDRESS,SALARY) " +
            "VALUES (1, 'Paul', 32, 'California', 20000.00 );";
        stmt.executeUpdate(sql);

        sql = "INSERT INTO " + tbName + " (ID,NAME,AGE,ADDRESS,SALARY) " +
            "VALUES (2, 'Allen', 25, 'Texas', 15000.00 );";
        stmt.executeUpdate(sql);

        sql = "INSERT INTO " + tbName + " (ID,NAME,AGE,ADDRESS,SALARY) " +
            "VALUES (3, 'Teddy', 23, 'Norway', 20000.00 );";
        stmt.executeUpdate(sql);

        sql = "INSERT INTO " + tbName + " (ID,NAME,AGE,ADDRESS,SALARY) " +
            "VALUES (4, 'Mark', 25, 'Rich-Mond', 65000.00 );";
        stmt.executeUpdate(sql);
      }
      catch (Throwable e) {
        DTThrowable.rethrow(e);
      }

    }

    private void cleanTable()
    {
        String sql = "delete from  " + dbloader.tableName;
        try {
          Statement stmt = dbloader.getConnection().createStatement();
          stmt.executeUpdate(sql);
          logger.debug("Table deleted successfully...");
        } catch (SQLException e) {
          DTThrowable.rethrow(e);
        }
    }

    @Override
    protected void finished(Description description)
    {
      cleanTable();
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testMysqlDBLookup() throws Exception
  {
    CountDownLatch latch = new CountDownLatch(1);

    ArrayList<String> lookupKeys = new ArrayList<String>();
    lookupKeys.add("ID");
    ArrayList<String> includeKeys = new ArrayList<String>();
    includeKeys.add("NAME");
    includeKeys.add("AGE");
    includeKeys.add("ADDRESS");
    testMeta.dbloader.setFields(lookupKeys, includeKeys);

    latch.await(1000, TimeUnit.MILLISECONDS);

    ArrayList<Object> keys = new ArrayList<Object>();
    keys.add("4");

    ArrayList<Object> columnInfo = (ArrayList<Object>) testMeta.dbloader.get(keys);

    Assert.assertEquals("NAME", "Mark", columnInfo.get(0).toString().trim());
    Assert.assertEquals("AGE", 25, columnInfo.get(1));
    Assert.assertEquals("ADDRESS", "Rich-Mond", columnInfo.get(2).toString().trim());
  }

  @Test
  public void testMysqlDBLookupIncludeAllKeys() throws Exception
  {
    CountDownLatch latch = new CountDownLatch(1);

    ArrayList<String> lookupKeys = new ArrayList<String>();
    lookupKeys.add("ID");
    ArrayList<String> includeKeys = new ArrayList<String>();
    testMeta.dbloader.setFields(lookupKeys, includeKeys);

    latch.await(1000, TimeUnit.MILLISECONDS);

    ArrayList<Object> keys = new ArrayList<Object>();
    keys.add("4");

    ArrayList<Object> columnInfo = (ArrayList<Object>) testMeta.dbloader.get(keys);

    Assert.assertEquals("ID", 4, columnInfo.get(0));
    Assert.assertEquals("NAME", "Mark", columnInfo.get(1).toString().trim());
    Assert.assertEquals("AGE", 25, columnInfo.get(2));
    Assert.assertEquals("ADDRESS", "Rich-Mond", columnInfo.get(3).toString().trim());
    Assert.assertEquals("SALARY", 65000.0, columnInfo.get(4));
  }

  @Test
  public void testMysqlDBQuery() throws Exception
  {
    CountDownLatch latch = new CountDownLatch(1);

    testMeta.dbloader.setQueryStmt("Select id, name from " + testMeta.dbloader.getTableName() + " where AGE = ? and ADDRESS = ?");

    latch.await(1000, TimeUnit.MILLISECONDS);

    ArrayList<Object> keys = new ArrayList<Object>();
    keys.add("25");
    keys.add("Texas");

    ArrayList<Object> columnInfo = (ArrayList<Object>) testMeta.dbloader.get(keys);

    Assert.assertEquals("ID", 2, columnInfo.get(0));
    Assert.assertEquals("NAME", "Allen", columnInfo.get(1).toString().trim());
  }
}
