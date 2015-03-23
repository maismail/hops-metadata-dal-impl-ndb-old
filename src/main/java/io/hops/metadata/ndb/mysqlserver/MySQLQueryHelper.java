package io.hops.metadata.ndb.mysqlserver;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import io.hops.exception.StorageException;

/**
 * This class is to do count operations using Mysql Server.
 */
public class MySQLQueryHelper {

  public static final String COUNT_QUERY = "select count(*) from %s";
  public static final String COUNT_QUERY_UNIQUE = "select count(distinct %s) from %s";
  public static final String SELECT_EXISTS= "select exists(%s)";
  public static final String SELECT_EXISTS_QUERY = "select * from %s";
  public static final String MIN = "select min(%s) from %s";
  public static final String MAX = "select min(%s) from %s";
  
  private static MysqlServerConnector connector = MysqlServerConnector.getInstance();

  /**
   * Counts the number of rows in a given table.
   *
   * This creates and closes connection in every request.
   *
   * @param tableName
   * @return Total number of rows a given table.
   * @throws io.hops.exception.StorageException
   */
  public static int countAll(String tableName) throws StorageException {
    // TODO[H]: Is it good to create and close connections in every call?
    String query = String.format(COUNT_QUERY, tableName);
    return executeIntAggrQuery(query);
  }
  
  public static int countAllUnique(String tableName, String columnName) throws StorageException{
    String query = String.format(COUNT_QUERY_UNIQUE, columnName, tableName);
    return executeIntAggrQuery(query);
  }
 
  /**
   * Counts the number of rows in a table specified by the table name where
   * satisfies the given criterion. The criterion should be a valid SLQ
   * statement.
   *
   * @param tableName
   * @param criterion E.g. criterion="id > 100".
   * @return
   */
  public static int countWithCriterion(String tableName, String criterion) throws StorageException {
    StringBuilder queryBuilder = new StringBuilder(String.format(COUNT_QUERY, tableName)).
            append(" where ").
            append(criterion);
    return executeIntAggrQuery(queryBuilder.toString());
  }
  
  public static boolean exists(String tableName, String criterion) throws StorageException {
    StringBuilder query = new StringBuilder(String.format(SELECT_EXISTS_QUERY, tableName));
    query.append(" where ").append(criterion);
    return executeBooleanQuery(String.format(SELECT_EXISTS, query.toString()));
  }

  public static int minInt(String tableName, String column, String criterion) throws StorageException{
    StringBuilder query = new StringBuilder(String.format(MIN, column, tableName));
    query.append(" where ").append(criterion);
    return executeIntAggrQuery(query.toString());
  }
  
  public static int maxInt(String tableName, String column, String criterion) throws StorageException {
    StringBuilder query = new StringBuilder(String.format(MAX, column, tableName));
    query.append(" where ").append(criterion);
    return executeIntAggrQuery(query.toString());
  }
    
  private static int executeIntAggrQuery(final String query) throws StorageException {
    return (Integer) execute(query, new ResultSetHandler() {
      @Override
      public Object checkResults(ResultSet result) throws SQLException {
        return result.getInt(1);
      }
    });
  }
  
  private static boolean executeBooleanQuery(final String query) throws StorageException {
    return (Boolean) execute(query, new ResultSetHandler() {
      @Override
      public Object checkResults(ResultSet result) throws SQLException {
        return result.getBoolean(1);
      }
    });
  }
  
  private static interface ResultSetHandler{
    Object checkResults(ResultSet result) throws SQLException;
  }
  
  private static Object execute(String query, ResultSetHandler handler) throws StorageException{
     try {
      Connection conn = connector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      ResultSet result = s.executeQuery();
      if(!result.next())
         throw new StorageException(String.format("result set is empty. Query: %s", query));
      return handler.checkResults(result);
    } catch (SQLException ex) {
      throw new StorageException(ex);
    } finally {
      connector.closeSession();
    }
  }
}
