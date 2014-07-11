package se.sics.hop.metadata.ndb;

import se.sics.hop.metadata.ndb.dalimpl.hdfs.LeaseClusterj;
import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.Constants;
import com.mysql.clusterj.LockMode;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import com.mysql.clusterj.Transaction;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.sics.hop.StorageConnector;
import se.sics.hop.metadata.hdfs.dal.BlockInfoDataAccess;
import se.sics.hop.metadata.hdfs.dal.CorruptReplicaDataAccess;
import se.sics.hop.metadata.hdfs.dal.EntityDataAccess;
import se.sics.hop.metadata.hdfs.dal.ExcessReplicaDataAccess;
import se.sics.hop.metadata.hdfs.dal.INodeAttributesDataAccess;
import se.sics.hop.metadata.hdfs.dal.INodeDataAccess;
import se.sics.hop.metadata.hdfs.dal.InvalidateBlockDataAccess;
import se.sics.hop.metadata.hdfs.dal.LeaderDataAccess;
import se.sics.hop.metadata.hdfs.dal.LeaseDataAccess;
import se.sics.hop.metadata.hdfs.dal.LeasePathDataAccess;
import se.sics.hop.metadata.hdfs.dal.PendingBlockDataAccess;
import se.sics.hop.metadata.hdfs.dal.ReplicaDataAccess;
import se.sics.hop.metadata.hdfs.dal.ReplicaUnderConstructionDataAccess;
import se.sics.hop.metadata.hdfs.dal.UnderReplicatedBlockDataAccess;
import se.sics.hop.metadata.hdfs.dal.VariableDataAccess;
import se.sics.hop.metadata.hdfs.entity.hop.var.HopVariable;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.dal.BlockLookUpDataAccess;
import se.sics.hop.metadata.hdfs.dal.StorageIdMapDataAccess;
import se.sics.hop.metadata.hdfs.tabledef.BlockInfoTableDef;
import se.sics.hop.metadata.hdfs.tabledef.BlockLookUpTableDef;
import se.sics.hop.metadata.hdfs.tabledef.CorruptReplicaTableDef;
import se.sics.hop.metadata.hdfs.tabledef.ExcessReplicaTableDef;
import se.sics.hop.metadata.hdfs.tabledef.INodeAttributesTableDef;
import se.sics.hop.metadata.hdfs.tabledef.INodeTableDef;
import se.sics.hop.metadata.hdfs.tabledef.InvalidatedBlockTableDef;
import se.sics.hop.metadata.hdfs.tabledef.LeaderTableDef;
import se.sics.hop.metadata.hdfs.tabledef.LeasePathTableDef;
import se.sics.hop.metadata.hdfs.tabledef.LeaseTableDef;
import se.sics.hop.metadata.hdfs.tabledef.PendingBlockTableDef;
import se.sics.hop.metadata.hdfs.tabledef.ReplicaTableDef;
import se.sics.hop.metadata.hdfs.tabledef.ReplicaUnderConstructionTableDef;
import se.sics.hop.metadata.hdfs.tabledef.StorageIdMapTableDef;
import se.sics.hop.metadata.hdfs.tabledef.UnderReplicatedBlockTableDef;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.BlockInfoClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.BlockLookUpClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.CorruptReplicaClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.ExcessReplicaClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.INodeAttributesClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.INodeClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.InvalidatedBlockClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.LeaderClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.LeasePathClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.PendingBlockClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.ReplicaClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.ReplicaUnderConstructionClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.StorageIdMapClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.UnderReplicatedBlockClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.VariableClusterj;
import se.sics.hop.metadata.ndb.mysqlserver.MysqlServerConnector;

public class ClusterjConnector implements StorageConnector<Session> {

  private static ClusterjConnector instance;
  
  static SessionFactory sessionFactory;
  static ThreadLocal<Session> sessionPool = new ThreadLocal<Session>();
  static final Log LOG = LogFactory.getLog(ClusterjConnector.class);
  
  private ClusterjConnector(){
    
  }
  
  public static ClusterjConnector getInstance(){
    if(instance == null){
      instance = new ClusterjConnector();
    }
    return instance;
  }
  
  @Override
  public void setConfiguration(Properties conf) throws StorageException {
    if (sessionFactory != null) {
      LOG.warn("SessionFactory is already initialized");
      return;
    }
    LOG.info("Database connect string: " + conf.get(Constants.PROPERTY_CLUSTER_CONNECTSTRING));
    LOG.info("Database name: " + conf.get(Constants.PROPERTY_CLUSTER_DATABASE));
    LOG.info("Max Transactions: " + conf.get(Constants.PROPERTY_CLUSTER_MAX_TRANSACTIONS));
    
    try {
      sessionFactory = ClusterJHelper.getSessionFactory(conf);
    } catch (ClusterJException ex) {
      throw new StorageException(ex);
    }
  }

  /*
   * Return a session from a random session factory in our pool.
   *
   * NOTE: Do not close the session returned by this call or you will die.
   */
  @Override
  public Session obtainSession() {
    Session session = sessionPool.get();
    if (session == null) {
      session = sessionFactory.getSession();
      sessionPool.set(session);
    }
    return session;
  }

  /**
   * begin a transaction.
   */
  @Override
  public void beginTransaction() throws StorageException {
    Session session = obtainSession();
    if(session.currentTransaction().isActive())
    {
      LOG.debug("Can not start Tx inside another Tx");
      System.exit(0);
    }
    session.currentTransaction().begin();
  }

  /**
   * Commit a transaction.
   */
  @Override
  public void commit() throws StorageException {
    Session session = obtainSession();
    Transaction tx = session.currentTransaction();
    if (!tx.isActive()) {
      throw new StorageException("The transaction is not began!");
    }

    tx.commit();
    session.flush();
    sessionPool.remove();
    session.close();
  }

  /**
   * It rolls back only when the transaction is active.
   */
  @Override
  public void rollback() {
    Session session = obtainSession();
    Transaction tx = session.currentTransaction();
    if (tx.isActive()) {
      tx.rollback();
    }
    sessionPool.remove();
    session.close();
  }

  /**
   * This is called only when MiniDFSCluster wants to format the Namenode.
   */
  @Override
  public boolean formatStorage() throws StorageException {
    return formatStorage(INodeDataAccess.class, BlockInfoDataAccess.class,
            LeaseDataAccess.class, LeasePathDataAccess.class, ReplicaDataAccess.class,
            ReplicaUnderConstructionDataAccess.class, InvalidateBlockDataAccess.class,
            ExcessReplicaDataAccess.class, PendingBlockDataAccess.class, CorruptReplicaDataAccess.class,
            UnderReplicatedBlockDataAccess.class, LeaderDataAccess.class, 
            INodeAttributesDataAccess.class, VariableDataAccess.class, StorageIdMapDataAccess.class, 
            BlockLookUpDataAccess.class);
  }

  @Override
  public boolean formatStorage(Class<? extends EntityDataAccess>... das) throws StorageException {
    Session session = obtainSession();
    Transaction tx = session.currentTransaction();
    session.setLockMode(LockMode.READ_COMMITTED);
    final int RETRIES = 5; // in test 
    for (int i = 0; i < RETRIES; i++) {
      try {
        tx.begin();
        for (Class e : das) {
          if (e == INodeDataAccess.class) {
            MysqlServerConnector.truncateTable(INodeTableDef.TABLE_NAME);

          } else if (e == BlockInfoDataAccess.class) {
            MysqlServerConnector.truncateTable(BlockInfoTableDef.TABLE_NAME);

          } else if (e == LeaseDataAccess.class) {
            MysqlServerConnector.truncateTable(LeaseTableDef.TABLE_NAME);
            
          } else if (e == LeasePathDataAccess.class) {
            MysqlServerConnector.truncateTable(LeasePathTableDef.TABLE_NAME);

          } else if (e == ReplicaDataAccess.class) {
            MysqlServerConnector.truncateTable(ReplicaTableDef.TABLE_NAME);

          } else if (e == ReplicaUnderConstructionDataAccess.class) {
            MysqlServerConnector.truncateTable(ReplicaUnderConstructionTableDef.TABLE_NAME);

          } else if (e == InvalidateBlockDataAccess.class) {
            MysqlServerConnector.truncateTable(InvalidatedBlockTableDef.TABLE_NAME);

          } else if (e == ExcessReplicaDataAccess.class) {
            MysqlServerConnector.truncateTable(ExcessReplicaTableDef.TABLE_NAME);

          } else if (e == PendingBlockDataAccess.class) {
            MysqlServerConnector.truncateTable(PendingBlockTableDef.TABLE_NAME);

          } else if (e == CorruptReplicaDataAccess.class) {
            MysqlServerConnector.truncateTable(CorruptReplicaTableDef.TABLE_NAME);

          } else if (e == UnderReplicatedBlockDataAccess.class) {
            MysqlServerConnector.truncateTable(UnderReplicatedBlockTableDef.TABLE_NAME);

          } else if (e == LeaderDataAccess.class) {
            MysqlServerConnector.truncateTable(LeaderTableDef.TABLE_NAME);
            
          } else if (e == INodeAttributesDataAccess.class) {
            MysqlServerConnector.truncateTable(INodeAttributesTableDef.TABLE_NAME);

          } else if (e == VariableDataAccess.class) {
            session.deletePersistentAll(VariableClusterj.VariableDTO.class);
            for (HopVariable.Finder varType : HopVariable.Finder.values()) {
              VariableClusterj.VariableDTO vd = session.newInstance(VariableClusterj.VariableDTO.class);
              vd.setId(varType.getId());
              vd.setValue(varType.getDefaultValue());
              session.savePersistent(vd);
            }
          }else if(e == StorageIdMapDataAccess.class){
            MysqlServerConnector.truncateTable(StorageIdMapTableDef.TABLE_NAME);
          }else if(e == BlockLookUpDataAccess.class){
            MysqlServerConnector.truncateTable(BlockLookUpTableDef.TABLE_NAME);
          }
        }
        MysqlServerConnector.truncateTable("path_memcached");
        tx.commit();
        session.flush();
        return true;

      } catch (Exception ex) {
        LOG.error(ex.getMessage(), ex);
        tx.rollback();
      }
    } // end retry loop
    return false;
  }
  
  @Override
  public boolean isTransactionActive() {
    return obtainSession().currentTransaction().isActive();
  }

  @Override
  public void stopStorage() {
  }

  @Override
  public void readLock() {
    Session session = obtainSession();
    session.setLockMode(LockMode.SHARED);
  }

  @Override
  public void writeLock() {
    Session session = obtainSession();
    session.setLockMode(LockMode.EXCLUSIVE);
  }

  @Override
  public void readCommitted() {
    Session session = obtainSession();
    session.setLockMode(LockMode.READ_COMMITTED);
  }
  
  @Override
  public void setPartitionKey(Class className, Object key) {
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

    Session session = obtainSession();
    session.setPartitionKey(cls, key);
  }
}
