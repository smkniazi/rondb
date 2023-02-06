/*
   Copyright (c) 2010, 2023, Oracle and/or its affiliates.
   Copyright (c) 2020, 2023, Hopsworks and/or its affiliates.

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

import com.mysql.clusterj.Constants;
import com.mysql.clusterj.DynamicObject;
import com.mysql.clusterj.Session;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

/*
Fixes for recreating a table with the same name while using session cache.
 */
public class UnloadSchemaAfterDeleteWithCacheTest extends AbstractClusterJModelTest {

  String DEFAULT_DB = "test";
  private static final String TABLE = "fgtest";
  private static String DROP_TABLE_CMD = "drop table if exists " + TABLE;

  private static String CREATE_TABLE_CMD1 = "CREATE TABLE " + TABLE + " ( id int NOT NULL," +
          " number1  int DEFAULT NULL,  number2  int DEFAULT NULL, PRIMARY KEY (id))" +
          " ENGINE=ndbcluster";

  // table with same name a above but different columns
  private static String CREATE_TABLE_CMD2 = "CREATE TABLE " + TABLE + " ( id int NOT NULL," +
          " number1  int DEFAULT NULL,number2  int DEFAULT NULL, number3  int DEFAULT NULL, " +
          "PRIMARY KEY (id)) ENGINE=ndbcluster";

  @Override
  protected Properties modifyProperties() {
    props.setProperty(Constants.PROPERTY_CLUSTER_MAX_CACHED_INSTANCES, Integer.toString(10));
    props.setProperty(Constants.PROPERTY_CLUSTER_WARMUP_CACHED_SESSIONS, Integer.toString(10));
    props.setProperty(Constants.PROPERTY_CLUSTER_MAX_CACHED_SESSIONS, Integer.toString(10));
    return props;
  }

  @Override
  public void localSetUp() {
    createSessionFactory();
    DEFAULT_DB = props.getProperty(Constants.PROPERTY_CLUSTER_DATABASE);
  }

  Session getSession(String db) {
    if (db == null) {
      return sessionFactory.getSession();
    } else {
      return sessionFactory.getSession(db);
    }
  }

  void returnSession(Session s) {
    s.closeCache();
  }

  void closeDTO(Session s, DynamicObject dto, Class dtoClass) {
    s.releaseCache(dto, dtoClass);
  }

  public static class FGTest1 extends DynamicObject {
    @Override
    public String table() {
      return TABLE;
    }
  }

  public static class FGTest2 extends DynamicObject {
    @Override
    public String table() {
      return TABLE;
    }
  }

  public void runSQLCMD(AbstractClusterJModelTest test, String cmd) {
    PreparedStatement preparedStatement = null;

    try {
      preparedStatement = connection.prepareStatement(cmd);
      preparedStatement.executeUpdate();
      System.out.println(cmd);
    } catch (SQLException e) {
      test.error("Failed to drop table. Error: " + e.getMessage());
      throw new RuntimeException("Failed to command: ", e);
    }
  }

  public void test() throws Exception {
    closeAllExistingSessionFactories();
    sessionFactory = null;
    createSessionFactory();

    runSQLCMD(this, DROP_TABLE_CMD);
    runSQLCMD(this, CREATE_TABLE_CMD2);

    // write something
    int tries = 1;
    Session session;
    DynamicObject dto;
    for (int i = 0; i < tries; i++) {
      session = getSession(DEFAULT_DB);
      dto = (DynamicObject) session.newInstance(FGTest1.class);
      setFields(this, dto, i);
      session.savePersistent(dto);
      closeDTO(session, dto, FGTest1.class);
      returnSession(session);
    }

    // delete the table and create a new table with the same name
    runSQLCMD(this, DROP_TABLE_CMD);
    runSQLCMD(this, CREATE_TABLE_CMD1);

    Session session1 = getSession(DEFAULT_DB);
    // unload schema
    session = getSession(DEFAULT_DB);
    System.out.println("Session 1: " + session1 + " session2: " + session);

    session.unloadSchema(FGTest2.class); // unload the schema using new dynamic class
    returnSession(session);

    // write something to the new table
    for (int i = 0; i < tries; i++) {
      session = session1;
//      session = getSession(DEFAULT_DB);
      dto = (DynamicObject) session.newInstance(FGTest2.class);
      setFields(this, dto, i);
      session.savePersistent(dto);
      closeDTO(session, dto, FGTest2.class);
      returnSession(session);
    }

    System.out.println("PASS");
  }

  public void setFields(AbstractClusterJModelTest test, DynamicObject e, int num) {
    for (int i = 0; i < e.columnMetadata().length; i++) {
      String fieldName = e.columnMetadata()[i].name();
      if (fieldName.equals("id")) {
        e.set(i, num);
      } else if (fieldName.startsWith("name")) {
        e.set(i, Integer.toString(num));
      } else if (fieldName.startsWith("number")) {
        e.set(i, num);
      } else {
        test.error("Unexpected Column");
      }
    }
  }
}
