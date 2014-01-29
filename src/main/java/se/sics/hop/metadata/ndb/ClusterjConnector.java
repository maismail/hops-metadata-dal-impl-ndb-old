package se.sics.hop.metadata.ndb;

import se.sics.hop.metadata.ndb.dalimpl.LeaseClusterj;
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
import se.sics.hop.metadata.ndb.dalimpl.BlockInfoClusterj;
import se.sics.hop.metadata.ndb.dalimpl.CorruptReplicaClusterj;
import se.sics.hop.metadata.ndb.dalimpl.ExcessReplicaClusterj;
import se.sics.hop.metadata.ndb.dalimpl.INodeAttributesClusterj;
import se.sics.hop.metadata.ndb.dalimpl.INodeClusterj;
import se.sics.hop.metadata.ndb.dalimpl.InvalidatedBlockClusterj;
import se.sics.hop.metadata.ndb.dalimpl.LeaderClusterj;
import se.sics.hop.metadata.ndb.dalimpl.LeasePathClusterj;
import se.sics.hop.metadata.ndb.dalimpl.PendingBlockClusterj;
import se.sics.hop.metadata.ndb.dalimpl.ReplicaClusterj;
import se.sics.hop.metadata.ndb.dalimpl.ReplicaUnderConstructionClusterj;
import se.sics.hop.metadata.ndb.dalimpl.UnderReplicatedBlockClusterj;
import se.sics.hop.metadata.ndb.dalimpl.VariableClusterj;

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
      LOG.info("New session object being obtained.");
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
            INodeAttributesDataAccess.class, VariableDataAccess.class);
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
            session.deletePersistentAll(INodeClusterj.InodeDTO.class);

          } else if (e == BlockInfoDataAccess.class) {
            session.deletePersistentAll(BlockInfoClusterj.BlockInfoDTO.class);

          } else if (e == LeaseDataAccess.class) {
            session.deletePersistentAll(LeaseClusterj.LeaseDTO.class);

          } else if (e == LeasePathDataAccess.class) {
            session.deletePersistentAll(LeasePathClusterj.LeasePathsDTO.class);

          } else if (e == ReplicaDataAccess.class) {
            session.deletePersistentAll(ReplicaClusterj.ReplicaDTO.class);

          } else if (e == ReplicaUnderConstructionDataAccess.class) {
            session.deletePersistentAll(ReplicaUnderConstructionClusterj.ReplicaUcDTO.class);

          } else if (e == InvalidateBlockDataAccess.class) {
            session.deletePersistentAll(InvalidatedBlockClusterj.InvalidateBlocksDTO.class);

          } else if (e == ExcessReplicaDataAccess.class) {
            session.deletePersistentAll(ExcessReplicaClusterj.ExcessReplicaDTO.class);

          } else if (e == PendingBlockDataAccess.class) {
            session.deletePersistentAll(PendingBlockClusterj.PendingBlockDTO.class);

          } else if (e == CorruptReplicaDataAccess.class) {
            session.deletePersistentAll(CorruptReplicaClusterj.CorruptReplicaDTO.class);

          } else if (e == UnderReplicatedBlockDataAccess.class) {
            session.deletePersistentAll(UnderReplicatedBlockClusterj.UnderReplicatedBlocksDTO.class);

          } else if (e == LeaderDataAccess.class) {
            session.deletePersistentAll(LeaderClusterj.LeaderDTO.class);

          } else if (e == INodeAttributesDataAccess.class) {
            session.deletePersistentAll(INodeAttributesClusterj.INodeAttributesDTO.class);

          } else if (e == VariableDataAccess.class) {
            session.deletePersistentAll(VariableClusterj.VariableDTO.class);
            for (HopVariable.Finder varType : HopVariable.Finder.values()) {
              VariableClusterj.VariableDTO vd = session.newInstance(VariableClusterj.VariableDTO.class);
              vd.setId(varType.getId());
              vd.setValue(varType.getDefaultValue());
              session.savePersistent(vd);
            }
          }
        }
        tx.commit();
        session.flush();
        return true;

      } catch (ClusterJException ex) {
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
