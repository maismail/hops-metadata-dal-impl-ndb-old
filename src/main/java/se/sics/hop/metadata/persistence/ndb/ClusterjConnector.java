package se.sics.hop.metadata.persistence.ndb;

import se.sics.hop.metadata.persistence.ndb.dalimpl.LeaseClusterj;
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
import se.sics.hop.metadata.persistence.StorageConnector;
import se.sics.hop.metadata.persistence.dal.BlockInfoDataAccess;
import se.sics.hop.metadata.persistence.dal.BlockTokenKeyDataAccess;
import se.sics.hop.metadata.persistence.dal.CorruptReplicaDataAccess;
import se.sics.hop.metadata.persistence.dal.EntityDataAccess;
import se.sics.hop.metadata.persistence.dal.ExcessReplicaDataAccess;
import se.sics.hop.metadata.persistence.dal.INodeAttributesDataAccess;
import se.sics.hop.metadata.persistence.dal.INodeDataAccess;
import se.sics.hop.metadata.persistence.dal.InvalidateBlockDataAccess;
import se.sics.hop.metadata.persistence.dal.LeaderDataAccess;
import se.sics.hop.metadata.persistence.dal.LeaseDataAccess;
import se.sics.hop.metadata.persistence.dal.LeasePathDataAccess;
import se.sics.hop.metadata.persistence.dal.PendingBlockDataAccess;
import se.sics.hop.metadata.persistence.dal.ReplicaDataAccess;
import se.sics.hop.metadata.persistence.dal.ReplicaUnderConstructionDataAccess;
import se.sics.hop.metadata.persistence.dal.StorageInfoDataAccess;
import se.sics.hop.metadata.persistence.dal.UnderReplicatedBlockDataAccess;
import se.sics.hop.metadata.persistence.dal.VariableDataAccess;
import se.sics.hop.metadata.persistence.entity.hop.var.HopVariable;
import se.sics.hop.metadata.persistence.exceptions.StorageException;
import se.sics.hop.metadata.persistence.ndb.dalimpl.BlockInfoClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.BlockTokenKeyClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.CorruptReplicaClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.ExcessReplicaClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.INodeAttributesClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.INodeClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.InvalidatedBlockClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.LeaderClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.LeasePathClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.PendingBlockClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.ReplicaClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.ReplicaUnderConstructionClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.StorageInfoClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.UnderReplicatedBlockClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.VariableClusterj;

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
            UnderReplicatedBlockDataAccess.class, LeaderDataAccess.class, BlockTokenKeyDataAccess.class,
            StorageInfoDataAccess.class, INodeAttributesDataAccess.class, VariableDataAccess.class);
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

          } else if (e == BlockTokenKeyDataAccess.class) {
            session.deletePersistentAll(BlockTokenKeyClusterj.BlockKeyDTO.class);

          } else if (e == StorageInfoDataAccess.class) {
            session.deletePersistentAll(StorageInfoClusterj.StorageInfoDTO.class);

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
    Session session = obtainSession();
    session.setPartitionKey(className, key);
  }
}
