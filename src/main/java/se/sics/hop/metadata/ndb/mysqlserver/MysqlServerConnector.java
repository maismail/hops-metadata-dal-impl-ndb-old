package se.sics.hop.metadata.ndb.mysqlserver;

import com.mysql.clusterj.Constants;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.log4j.Logger;
import se.sics.hop.StorageConnector;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.dal.EntityDataAccess;

/**
 * This class presents a singleton connector to Mysql Server.
 * It creates connections to Mysql Server and loads the driver.
 * 
 * @author hooman
 */
public class MysqlServerConnector implements StorageConnector<Connection> {

  private static MysqlServerConnector instance;
  private Logger log;
  private String protocol;
  private String user;
  private String password;
  private ThreadLocal<Connection> connectionPool = new ThreadLocal<Connection>();
  public static final String DRIVER = "com.mysql.jdbc.Driver";

  private MysqlServerConnector() {
    log = Logger.getLogger(MysqlServerConnector.class);
  }

  public static MysqlServerConnector getInstance(){
    if(instance == null){
      instance = new MysqlServerConnector();
    }
    return instance;
  }
  
  @Override
  public void setConfiguration(Properties conf) throws StorageException {
    this.protocol = conf.getProperty(Constants.PROPERTY_JDBC_URL);
    this.user = conf.getProperty(Constants.PROPERTY_JDBC_USERNAME);
    this.password = conf.getProperty(Constants.PROPERTY_JDBC_PASSWORD);
    loadDriver();
  }

  private void loadDriver() throws StorageException {
    try {
      Class.forName(DRIVER).newInstance();
      log.info("Loaded Mysql driver.");
    } catch (ClassNotFoundException cnfe) {
      log.error("\nUnable to load the JDBC driver " + DRIVER, cnfe);
      throw new StorageException(cnfe);
    } catch (InstantiationException ie) {
      log.error("\nUnable to instantiate the JDBC driver " + DRIVER, ie);
      throw new StorageException(ie);
    } catch (IllegalAccessException iae) {
      log.error("\nNot allowed to access the JDBC driver " + DRIVER, iae);
      throw new StorageException(iae);
    }
  }

  @Override
  public Connection obtainSession() throws StorageException {
    Connection conn = connectionPool.get();
    if (conn == null) {
      try {
        conn = DriverManager.getConnection(protocol, user, password);
        connectionPool.set(conn);
      } catch (SQLException ex) {
        throw new StorageException(ex);
      }
    }
    return conn;
  }
  
  public void closeSession() throws StorageException
  {
    Connection conn = connectionPool.get();
    if (conn != null) {
      try {
        conn.close();
        connectionPool.remove();
      } catch (SQLException ex) {
        throw new StorageException(ex);
      }
    }
  }
  
  public static void truncateTable(String tableName) throws StorageException, SQLException{
    MysqlServerConnector connector = MysqlServerConnector.getInstance();
    try {
      Connection conn = connector.obtainSession();
      PreparedStatement s = conn.prepareStatement("delete from "+tableName);
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
}
