package se.sics.hop.metadata.ndb;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.Constants;
import com.mysql.clusterj.LockMode;
import com.mysql.clusterj.Transaction;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.sics.hop.StorageConnector;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.dal.*;
import se.sics.hop.metadata.hdfs.entity.hop.var.HopVariable;
import se.sics.hop.metadata.hdfs.tabledef.*;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.*;
import se.sics.hop.metadata.ndb.mysqlserver.MysqlServerConnector;

import java.util.Properties;

public class ClusterjConnector implements StorageConnector<DBSession> {

  private static ClusterjConnector instance;
  private static boolean isInitialized = false;
  private DBSessionProvider dbSessionProvider = null;
  static ThreadLocal<DBSession> sessions = new ThreadLocal<DBSession>();
  static final Log LOG = LogFactory.getLog(ClusterjConnector.class);

  private ClusterjConnector() {
  }

  public static ClusterjConnector getInstance() {
    if (instance == null) {
      instance = new ClusterjConnector();
    }
    return instance;
  }

  @Override
  public void setConfiguration(Properties conf) throws StorageException {
    if (isInitialized) {
      LOG.warn("SessionFactory is already initialized");
      return;
    }
    LOG.info("Database connect string: " + conf.get(Constants.PROPERTY_CLUSTER_CONNECTSTRING));
    LOG.info("Database name: " + conf.get(Constants.PROPERTY_CLUSTER_DATABASE));
    LOG.info("Max Transactions: " + conf.get(Constants.PROPERTY_CLUSTER_MAX_TRANSACTIONS));

    try {
      int initialPoolSize = Integer.parseInt((String) conf.get("se.kth.hop.session.pool.size"));
      int reuseCount = Integer.parseInt((String) conf.get("se.kth.hop.session.reuse.count"));
      dbSessionProvider = new DBSessionProvider(conf, reuseCount, initialPoolSize);

      isInitialized = true;
    } catch (ClusterJException ex) {
      throw new StorageException(ex);
    }
  }

  /*
   * Return a dbSession from a random dbSession factory in our pool.
   *
   * NOTE: Do not close the dbSession returned by this call or you will die.
   */
  @Override
  public DBSession obtainSession() throws StorageException {
    try {
      DBSession dbSession = sessions.get();
      if (dbSession == null) {
        dbSession = dbSessionProvider.getSession();
        sessions.set(dbSession);
      }
      return dbSession;
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private void returnSession(boolean error) throws StorageException {
    DBSession dbSession = obtainSession();
    sessions.remove(); // remove, and return to the pool
    dbSessionProvider.returnSession(dbSession, error); // if there was an error then close the session
  }

  /**
   * begin a transaction.
   */
  @Override
  public void beginTransaction() throws StorageException {
    try {
      DBSession dbSession = obtainSession();
      if (dbSession.getSession().currentTransaction().isActive()) {
        StringBuilder msg = new StringBuilder();
        msg.append("Can not start Tx inside another Tx:\n");
        for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
          msg.append("\t" + element + "\n");
        }
        LOG.fatal(msg);
        System.exit(-1);
      }
      dbSession.getSession().currentTransaction().begin();
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  /**
   * Commit a transaction.
   */
  @Override
  public void commit() throws StorageException {
    DBSession dbSession = null;
    boolean dbError = false;
    try {
      dbSession = obtainSession();
      Transaction tx = dbSession.getSession().currentTransaction();
      if (!tx.isActive()) {
        throw new StorageException("The transaction is not began!");
      }
      tx.commit();
    } catch (Exception e) {
      dbError = true;
      throw new StorageException(e);
    } finally {
      returnSession(dbError);
    }
  }

  /**
   * It rolls back only when the transaction is active.
   */
  @Override
  public void rollback() throws StorageException {
    DBSession dbSession = null;
    boolean dbError = false;
    try {
      dbSession = obtainSession();
      Transaction tx = dbSession.getSession().currentTransaction();
      if (tx.isActive()) {
        tx.rollback();
      }
    } catch (Exception e) {
      dbError = true;
      throw new StorageException(e);
    } finally {
      returnSession(dbError);
    }
  }

  /**
   * This is called only when MiniDFSCluster wants to format the Namenode.
   */
  @Override
  public boolean formatStorage() throws StorageException {
    return format(true);
  }

  @Override
  public boolean formatStorage(Class<? extends EntityDataAccess>... das) throws StorageException {
    return format(true, das);
  }

  @Override
  public boolean isTransactionActive() throws StorageException {
    try {
      return obtainSession().getSession().currentTransaction().isActive();
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void stopStorage() throws StorageException {
    dbSessionProvider.stop();
  }

  @Override
  public void readLock() throws StorageException {
    try {
      DBSession dbSession = obtainSession();
      dbSession.getSession().setLockMode(LockMode.SHARED);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void writeLock() throws StorageException {
    try {
      DBSession dbSession = obtainSession();
      dbSession.getSession().setLockMode(LockMode.EXCLUSIVE);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void readCommitted() throws StorageException {
    try {
      DBSession dbSession = obtainSession();
      dbSession.getSession().setLockMode(LockMode.READ_COMMITTED);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void setPartitionKey(Class className, Object key) throws StorageException {
    Class cls = null;
    if (className == BlockInfoDataAccess.class) {
      cls = BlockInfoClusterj.BlockInfoDTO.class;
    } else if (className == PendingBlockDataAccess.class) {
      cls = PendingBlockClusterj.PendingBlockDTO.class;
    } else if (className == ReplicaUnderConstructionDataAccess.class) {
      cls = ReplicaUnderConstructionClusterj.ReplicaUcDTO.class;
    } else if (className == INodeDataAccess.class) {
      cls = INodeClusterj.InodeDTO.class;
    } else if (className == INodeAttributesDataAccess.class) {
      cls = INodeAttributesClusterj.INodeAttributesDTO.class;
    } else if (className == LeaseDataAccess.class) {
      cls = LeaseClusterj.LeaseDTO.class;
    } else if (className == LeasePathDataAccess.class) {
      cls = LeasePathClusterj.LeasePathsDTO.class;
    } else if (className == LeaderDataAccess.class) {
      cls = LeaderClusterj.LeaderDTO.class;
    } else if (className == ReplicaDataAccess.class) {
      cls = ReplicaClusterj.ReplicaDTO.class;
    } else if (className == CorruptReplicaDataAccess.class) {
      cls = CorruptReplicaClusterj.CorruptReplicaDTO.class;
    } else if (className == ExcessReplicaDataAccess.class) {
      cls = ExcessReplicaClusterj.ExcessReplicaDTO.class;
    } else if (className == InvalidateBlockDataAccess.class) {
      cls = InvalidatedBlockClusterj.InvalidateBlocksDTO.class;
    } else if (className == UnderReplicatedBlockDataAccess.class) {
      cls = UnderReplicatedBlockClusterj.UnderReplicatedBlocksDTO.class;
    } else if (className == VariableDataAccess.class) {
      cls = VariableClusterj.VariableDTO.class;
    }

    try {
      DBSession dbSession = obtainSession();
      dbSession.getSession().setPartitionKey(cls, key);
      dbSession.getSession().flush();
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public boolean formatStorageNonTransactional() throws StorageException {
    return format(false);
  }

  private boolean format(boolean transactional) throws StorageException {
    return format(transactional, VariableDataAccess.class, INodeDataAccess.class, BlockInfoDataAccess.class,
            LeaseDataAccess.class, LeasePathDataAccess.class, ReplicaDataAccess.class,
            ReplicaUnderConstructionDataAccess.class, InvalidateBlockDataAccess.class,
            ExcessReplicaDataAccess.class, PendingBlockDataAccess.class, CorruptReplicaDataAccess.class,
            UnderReplicatedBlockDataAccess.class, LeaderDataAccess.class,
            INodeAttributesDataAccess.class, StorageIdMapDataAccess.class,
            BlockLookUpDataAccess.class, SafeBlocksDataAccess.class, MisReplicatedRangeQueueDataAccess.class, QuotaUpdateDataAccess.class);
  }

  private boolean format(boolean transactional, Class<? extends EntityDataAccess>... das) throws StorageException {
    final int RETRIES = 5; // in test 
    for (int i = 0; i < RETRIES; i++) {
      try {
        for (Class e : das) {
          if (e == INodeDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional, INodeTableDef.TABLE_NAME);

          } else if (e == BlockInfoDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional, BlockInfoTableDef.TABLE_NAME);

          } else if (e == LeaseDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional, LeaseTableDef.TABLE_NAME);

          } else if (e == LeasePathDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional, LeasePathTableDef.TABLE_NAME);

          } else if (e == ReplicaDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional, ReplicaTableDef.TABLE_NAME);

          } else if (e == ReplicaUnderConstructionDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional, ReplicaUnderConstructionTableDef.TABLE_NAME);

          } else if (e == InvalidateBlockDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional, InvalidatedBlockTableDef.TABLE_NAME);

          } else if (e == ExcessReplicaDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional, ExcessReplicaTableDef.TABLE_NAME);

          } else if (e == PendingBlockDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional, PendingBlockTableDef.TABLE_NAME);

          } else if (e == CorruptReplicaDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional, CorruptReplicaTableDef.TABLE_NAME);

          } else if (e == UnderReplicatedBlockDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional, UnderReplicatedBlockTableDef.TABLE_NAME);

          } else if (e == LeaderDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional, LeaderTableDef.TABLE_NAME);

          } else if (e == INodeAttributesDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional, INodeAttributesTableDef.TABLE_NAME);

          } else if (e == VariableDataAccess.class) {
            DBSession dbSession = obtainSession();
            dbSession.getSession().currentTransaction().begin();
            dbSession.getSession().deletePersistentAll(VariableClusterj.VariableDTO.class);
            for (HopVariable.Finder varType : HopVariable.Finder.values()) {
              VariableClusterj.VariableDTO vd = dbSession.getSession().newInstance(VariableClusterj.VariableDTO.class);
              vd.setId(varType.getId());
              vd.setValue(varType.getDefaultValue());
              dbSession.getSession().savePersistent(vd);
            }
            dbSession.getSession().currentTransaction().commit();
          } else if (e == StorageIdMapDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional, StorageIdMapTableDef.TABLE_NAME);
          } else if (e == BlockLookUpDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional, BlockLookUpTableDef.TABLE_NAME);
          } else if (e == SafeBlocksDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional, SafeBlocksTableDef.TABLE_NAME);
          } else if (e == MisReplicatedRangeQueueDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional, MisReplicatedRangeQueueTableDef.TABLE_NAME);
          } else if (e == QuotaUpdateDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional, QuotaUpdateTableDef.TABLE_NAME);
          }
        }
        MysqlServerConnector.truncateTable(transactional, "path_memcached");
        return true;

      } catch (Exception ex) {
        LOG.error(ex.getMessage(), ex);
      }
    } // end retry loop
    return false;
  }
}
