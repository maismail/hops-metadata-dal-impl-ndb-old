package se.sics.hop.metadata.ndb.mysqlserver;

import com.mysql.clusterj.Constants;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import se.sics.hop.StorageConnector;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.dal.EntityDataAccess;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

/**
 * This class presents a singleton connector to Mysql Server.
 * It creates connections to Mysql Server and loads the driver.
 * 
 * @author hooman
 */
public class MysqlServerConnector implements StorageConnector<Connection> {

  private static MysqlServerConnector instance;
  private static HikariDataSource connectionPool;
  private ThreadLocal<Connection> connection = new ThreadLocal<Connection>();

  public static MysqlServerConnector getInstance(){
    if(instance == null){
      instance = new MysqlServerConnector();
    }
    return instance;
  }
  
  @Override
  public void setConfiguration(Properties conf) throws StorageException {
    initializeConnectionPool(conf);
  }

  private void initializeConnectionPool(Properties conf) {
    HikariConfig config = new HikariConfig();
    config.setMaximumPoolSize(Integer.valueOf(
        conf.getProperty(se.sics.hop.metadata.ndb.mysqlserver.Constants.PROPERTY_MYSQL_CONNECTION_POOL_SIZE)));
    config.setDataSourceClassName(
        conf.getProperty(se.sics.hop.metadata.ndb.mysqlserver.Constants.PROPERTY_MYSQL_DATA_SOURCE_CLASS_NAME));
    config.addDataSourceProperty("serverName",
        conf.getProperty(se.sics.hop.metadata.ndb.mysqlserver.Constants.PROPERTY_MYSQL_HOST));
    config.addDataSourceProperty("port",
        conf.getProperty(se.sics.hop.metadata.ndb.mysqlserver.Constants.PROPERTY_MYSQL_PORT));
    config.addDataSourceProperty("databaseName",
        conf.getProperty(Constants.PROPERTY_CLUSTER_DATABASE));
    config.addDataSourceProperty("user",
        conf.getProperty(se.sics.hop.metadata.ndb.mysqlserver.Constants.PROPERTY_MYSQL_USERNAME));
    config.addDataSourceProperty("password",
        conf.getProperty(se.sics.hop.metadata.ndb.mysqlserver.Constants.PROPERTY_MYSQL_PASSWORD));

    connectionPool = new HikariDataSource(config);
  }

  @Override
  public Connection obtainSession() throws StorageException {
    Connection conn = connection.get();
    if (conn == null) {
      try {
        conn = connectionPool.getConnection();
        connection.set(conn);
      } catch (SQLException ex) {
        throw new StorageException(ex);
      }
    }
    return conn;
  }

  public void closeSession() throws StorageException
  {
    Connection conn = connection.get();
    if (conn != null) {
      try {
        conn.close();
        connection.remove();
      } catch (SQLException ex) {
        throw new StorageException(ex);
      }
    }
  }
  
  public static void truncateTable(boolean transactional, String tableName) throws StorageException, SQLException {
    truncateTable(transactional, tableName, -1);
  }

  public static void truncateTable(String tableName, int limit) throws StorageException, SQLException {
    truncateTable(true, tableName, -1);
  }

  public static void truncateTable(boolean transactional, String tableName, int limit) throws StorageException, SQLException{
    MysqlServerConnector connector = MysqlServerConnector.getInstance();
    try {
      Connection conn = connector.obtainSession();
      PreparedStatement s = transactional ? conn.prepareStatement("delete from "+tableName + (limit > 0 ? " limit " + limit : "") ) : conn.prepareStatement("truncate table "+tableName);
      s.executeUpdate();
    } finally {
      connector.closeSession();
    }
  }

  @Override
  public void beginTransaction() throws StorageException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void commit() throws StorageException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void rollback() throws StorageException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean formatStorage() throws StorageException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public boolean formatStorage(Class<? extends EntityDataAccess>... das) throws StorageException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
  
  @Override
  public boolean isTransactionActive()  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void stopStorage() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void readLock() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void writeLock() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void readCommitted() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void setPartitionKey(Class className, Object key) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean formatStorageNonTransactional() throws StorageException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
}
