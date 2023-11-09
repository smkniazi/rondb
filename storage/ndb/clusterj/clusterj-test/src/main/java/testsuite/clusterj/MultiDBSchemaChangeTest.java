/*
   Copyright (c) 2022 Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
   */

package testsuite.clusterj;

import com.mysql.clusterj.ClusterJDatastoreException;
import com.mysql.clusterj.Constants;
import com.mysql.clusterj.DynamicObject;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.core.store.ClusterConnection;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;


/**
 * multiple databases with similar tables. later the tables are altered
 */
public class MultiDBSchemaChangeTest extends AbstractClusterJModelTest {
  private static final String TABLE = "multi_db_table";
  private static final String DB_PREFIX = "db";

  boolean useCache = true;
  private static final int NUM_THREADS = 5;

  @Override
  protected Properties modifyProperties() {
    if (useCache) {
      props.setProperty(Constants.PROPERTY_CLUSTER_MAX_CACHED_INSTANCES, Integer.toString(NUM_THREADS));
      props.setProperty(Constants.PROPERTY_CLUSTER_WARMUP_CACHED_SESSIONS, Integer.toString(NUM_THREADS));
      props.setProperty(Constants.PROPERTY_CLUSTER_MAX_CACHED_SESSIONS, Integer.toString(NUM_THREADS));
    } else {
      props.setProperty(Constants.PROPERTY_CLUSTER_MAX_CACHED_INSTANCES, "0");
      props.setProperty(Constants.PROPERTY_CLUSTER_WARMUP_CACHED_SESSIONS, "0");
      props.setProperty(Constants.PROPERTY_CLUSTER_MAX_CACHED_SESSIONS, "0");
    }
    return props;
  }

  @Override
  public void localSetUp() {
    createSessionFactory();
  }

  Session getSession(String db) {
    if (db == null) {
      return sessionFactory.getSession();
    } else {
      return sessionFactory.getSession(db);
    }
  }

  void returnSession(Session s) {
    if (useCache) {
      s.closeCache();
    } else {
      s.close();
    }
  }

  void closeDTO(Session s, DynamicObject dto, Class dtoClass) {
    if (useCache) {
      s.releaseCache(dto, dtoClass);
    } else {
      s.release(dto);
    }
  }

  @PersistenceCapable(table=TABLE)
  public interface TableDTO {
    @PrimaryKey
    public int getId();
    public int setId(int id);
  }



  private void setUpDB(String DB) {
    runSQLCMD(this, "DROP DATABASE IF EXISTS "+DB);
    runSQLCMD(this, "CREATE DATABASE  "+DB);
    String createTableCmd = "CREATE TABLE " + DB+"."+TABLE + " ( id int NOT NULL, col_old_1 " +
      "INT DEFAULT NULL, col_old_2 INT DEFAULT NULL,col_old_3 INT DEFAULT NULL, col_old_4 INT DEFAULT NULL, " +
      " PRIMARY KEY (id)) ENGINE=ndbcluster";
    runSQLCMD(this, createTableCmd);
  }

  private void updateDB(String DB, int colID) {
    String alterTableCmd = "ALTER TABLE " + DB+"."+TABLE + " ADD COLUMN col_new_"+colID+" INT DEFAULT NULL,  algorithm=COPY";
    runSQLCMD(this, alterTableCmd);
  }

  private void tearDownDB(String DB) {
    runSQLCMD(this, "DROP DATABASE IF EXISTS "+DB);
  }

  public void runSQLCMD(AbstractClusterJModelTest test, String cmd) {
    PreparedStatement preparedStatement = null;

    try {
      System.out.println(cmd);
      preparedStatement = connection.prepareStatement(cmd);
      preparedStatement.executeUpdate();
    } catch (SQLException e) {
      System.err.println("Failed to run SQL command. "+e);
      test.fail("Failed to drop table. Error: " + e.getMessage());
    }
  }

  public void testMultiDBSchemaChange1() {
    multiDBSchemaChange(true);
  }

  public void multiDBSchemaChange(boolean useCache) {


    try {
      this.useCache = useCache;
      closeSession();
      closeAllExistingSessionFactories();
      sessionFactory = null;
      createSessionFactory();

      int num_dbs = 3;
      for (int i = 0 ; i < num_dbs; i++){
        setUpDB(DB_PREFIX+i);
      }

      Random rand= new Random();

      List<DataInsertWorker> threads = new ArrayList<>(NUM_THREADS);
      for (int i = 0; i < NUM_THREADS; i++) {
        DataInsertWorker t = new DataInsertWorker(DB_PREFIX+Math.abs((rand.nextInt()%num_dbs)), i * 1000000, 10000);
        threads.add(t);
        t.start();
      }

      Thread.sleep(2000);

      for (int j = 0; j < 5; j++) {
        for (int i = 0 ; i < num_dbs; i++){
          updateDB(DB_PREFIX+i, j);
          Thread.sleep(200);
        }
      }

      Thread.sleep(2000);


      int failed = 0;
      int success = 0;
      for (int i = 0; i < NUM_THREADS; i++) {
        threads.get(i).stopDataInsertion();
        threads.get(i).join();
        failed += threads.get(i).getFailCounter();
        success += threads.get(i).getInsertsCounter();
      }

      for (int i = 0 ; i < num_dbs; i++){
        //tearDownDB(DB_PREFIX+i);
      }

      failOnError();
      System.out.println("Finished. Failed: "+failed+" Success: "+success);
    } catch (Exception e) {
      System.err.println("FAILED . Error: " + e.getMessage());
      e.printStackTrace();
      this.fail("FAILED . Error: " + e.getMessage());
    }
  }

  public static class TestTable extends DynamicObject {
    @Override
    public String table() {
      return TABLE;
    }
  }

  class DataInsertWorker extends Thread {


    private boolean run = true;
    private int startIndex = 0;
    private String db;

    private int maxRowsToWrite = 0;
    private int insertsCounter = 0;
    private int failCounter = 0;

    DataInsertWorker(String db, int startIndex, int maxRowsToWrite) {
      this.db = db;
      System.out.println(db);
      this.startIndex = startIndex;
      this.maxRowsToWrite = maxRowsToWrite;
    }

    @Override
    public void run() {

      int currentIndex = startIndex;
      while (run) {
        Session session = getSession(db);
        DynamicObject e = null;
        boolean rowInserted = false;
        try {
          e = (DynamicObject) session.newInstance(TestTable.class);
          setFields(e, currentIndex++);
          session.savePersistent(e);
          closeDTO(session, e, TestTable.class);
          insertsCounter++;
          rowInserted = true;
          if (currentIndex > (startIndex + maxRowsToWrite)) {
            currentIndex = startIndex;
          }
        } catch (Exception ex) {
          System.out.println(ex.getMessage());
          if (!( ex instanceof ClusterJDatastoreException )) {
            ex.printStackTrace();
          }
          failCounter++;
        } finally {
          if (!rowInserted) {
            System.out.println("unload schema called");
            session.unloadSchema(TestTable.class);
            session.close();
            try {
              Thread.sleep(5);
            } catch (InterruptedException ex) {
              throw new RuntimeException(ex);
            }
          } else {
            returnSession(session);
          }
        }
      }
    }

    public void stopDataInsertion() {
      run = false;
    }

    public int getInsertsCounter() {
      return insertsCounter;
    }

    public int getFailCounter() {
      return failCounter;
    }

    public void setFields(DynamicObject e, int num) {
      for (int i = 0; i < e.columnMetadata().length; i++) {
        String fieldName = e.columnMetadata()[i].name();
        if (fieldName.equals("id")) {
          e.set(i, num);
        } else if (fieldName.startsWith("col")) {
          e.set(i, num);
        } else {
          throw new IllegalArgumentException("Unexpected Column");
        }
      }
    }
  }
}
