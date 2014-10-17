package se.sics.hop.metadata.ndb;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.Constants;
import com.mysql.clusterj.LockMode;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import com.mysql.clusterj.Transaction;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.log4j.Logger;
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
import se.sics.hop.metadata.ndb.dalimpl.hdfs.CorruptReplicaClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.ExcessReplicaClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.INodeAttributesClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.INodeClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.InvalidatedBlockClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.LeaderClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.LeaseClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.LeasePathClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.PendingBlockClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.ReplicaClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.ReplicaUnderConstructionClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.UnderReplicatedBlockClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.VariableClusterj;
import se.sics.hop.metadata.ndb.dalimpl.yarn.YarnVariablesClusterJ;
import se.sics.hop.metadata.yarn.dal.AppSchedulingInfoBlacklistDataAccess;
import se.sics.hop.metadata.yarn.dal.AppSchedulingInfoDataAccess;
import se.sics.hop.metadata.yarn.dal.rmstatestore.AppMasterRPCDataAccess;
import se.sics.hop.metadata.yarn.dal.ContainerDataAccess;
import se.sics.hop.metadata.yarn.dal.ContainerIdToCleanDataAccess;
import se.sics.hop.metadata.yarn.dal.ContainerStatusDataAccess;
import se.sics.hop.metadata.yarn.dal.capacity.FiCaSchedulerAppLastScheduledContainerDataAccess;
import se.sics.hop.metadata.yarn.dal.FiCaSchedulerAppLiveContainersDataAccess;
import se.sics.hop.metadata.yarn.dal.FiCaSchedulerAppNewlyAllocatedContainersDataAccess;
import se.sics.hop.metadata.yarn.dal.capacity.FiCaSchedulerAppReservationsDataAccess;
import se.sics.hop.metadata.yarn.dal.capacity.FiCaSchedulerAppReservedContainersDataAccess;
import se.sics.hop.metadata.yarn.dal.capacity.FiCaSchedulerAppSchedulingOpportunitiesDataAccess;
import se.sics.hop.metadata.yarn.dal.FiCaSchedulerNodeDataAccess;
import se.sics.hop.metadata.yarn.dal.FifoSchedulerNodesDataAccess;
import se.sics.hop.metadata.yarn.dal.FinishedApplicationsDataAccess;
import se.sics.hop.metadata.yarn.dal.JustLaunchedContainersDataAccess;
import se.sics.hop.metadata.yarn.dal.LaunchedContainersDataAccess;
import se.sics.hop.metadata.yarn.dal.NodeDataAccess;
import se.sics.hop.metadata.yarn.dal.QueueMetricsDataAccess;
import se.sics.hop.metadata.yarn.dal.RMContainerDataAccess;
import se.sics.hop.metadata.yarn.dal.RMContextInactiveNodesDataAccess;
import se.sics.hop.metadata.yarn.dal.RMContextActiveNodesDataAccess;
import se.sics.hop.metadata.yarn.dal.RMNodeDataAccess;
import se.sics.hop.metadata.yarn.dal.ResourceDataAccess;
import se.sics.hop.metadata.yarn.dal.ResourceRequestDataAccess;
import se.sics.hop.metadata.yarn.dal.SchedulerApplicationDataAccess;
import se.sics.hop.metadata.yarn.dal.TokenDataAccess;
import se.sics.hop.metadata.yarn.dal.UpdatedContainerInfoDataAccess;
import se.sics.hop.metadata.yarn.dal.YarnVariablesDataAccess;
import se.sics.hop.metadata.yarn.dal.rmstatestore.ApplicationAttemptStateDataAccess;
import se.sics.hop.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import se.sics.hop.metadata.yarn.dal.rmstatestore.DelegationKeyDataAccess;
import se.sics.hop.metadata.yarn.dal.rmstatestore.DelegationTokenDataAccess;
import se.sics.hop.metadata.yarn.dal.rmstatestore.RMStateVersionDataAccess;
import se.sics.hop.metadata.yarn.dal.rmstatestore.SequenceNumberDataAccess;
import se.sics.hop.metadata.yarn.tabledef.AppSchedulingInfoBlacklistTableDef;
import se.sics.hop.metadata.yarn.tabledef.AppSchedulingInfoTableDef;
import se.sics.hop.metadata.yarn.tabledef.ContainerIdToCleanTableDef;
import se.sics.hop.metadata.yarn.tabledef.ContainerStatusTableDef;
import se.sics.hop.metadata.yarn.tabledef.ContainerTableDef;
import se.sics.hop.metadata.yarn.tabledef.capacity.FiCaSchedulerAppLastScheduledContainerTableDef;
import se.sics.hop.metadata.yarn.tabledef.FiCaSchedulerAppLiveContainersTableDef;
import se.sics.hop.metadata.yarn.tabledef.FiCaSchedulerAppNewlyAllocatedContainersTableDef;
import se.sics.hop.metadata.yarn.tabledef.capacity.FiCaSchedulerAppReservationsTableDef;
import se.sics.hop.metadata.yarn.tabledef.capacity.FiCaSchedulerAppReservedContainersTableDef;
import se.sics.hop.metadata.yarn.tabledef.capacity.FiCaSchedulerAppSchedulingOpportunitiesTableDef;
import se.sics.hop.metadata.yarn.tabledef.FiCaSchedulerNodeTableDef;
import se.sics.hop.metadata.yarn.tabledef.FifoSchedulerNodesTableDef;
import se.sics.hop.metadata.yarn.tabledef.FinishedApplicationsTableDef;
import se.sics.hop.metadata.yarn.tabledef.JustLaunchedContainersTableDef;
import se.sics.hop.metadata.yarn.tabledef.LaunchedContainersTableDef;
import se.sics.hop.metadata.yarn.tabledef.NodeTableDef;
import se.sics.hop.metadata.yarn.tabledef.QueueMetricsTableDef;
import se.sics.hop.metadata.yarn.tabledef.RMContainerTableDef;
import se.sics.hop.metadata.yarn.tabledef.RMContextInactiveNodesTableDef;
import se.sics.hop.metadata.yarn.tabledef.RMContextActiveNodesTableDef;
import se.sics.hop.metadata.yarn.tabledef.RMNodeTableDef;
import se.sics.hop.metadata.yarn.tabledef.ResourceRequestTableDef;
import se.sics.hop.metadata.yarn.tabledef.ResourceTableDef;
import se.sics.hop.metadata.yarn.tabledef.SchedulerApplicationTableDef;
import se.sics.hop.metadata.yarn.tabledef.TokenTableDef;
import se.sics.hop.metadata.yarn.tabledef.UpdatedContainerInfoTableDef;
import se.sics.hop.metadata.yarn.tabledef.appmasterrpc.AppMasterRPCTableDef;
import se.sics.hop.metadata.yarn.tabledef.rmstatestore.ApplicationAttemptStateTableDef;
import se.sics.hop.metadata.yarn.tabledef.rmstatestore.ApplicationStateTableDef;
import se.sics.hop.metadata.yarn.tabledef.rmstatestore.DelegationKeyTableDef;
import se.sics.hop.metadata.yarn.tabledef.rmstatestore.DelegationTokenTableDef;
import se.sics.hop.metadata.yarn.tabledef.rmstatestore.SequenceNumberTableDef;
import se.sics.hop.metadata.yarn.tabledef.rmstatestore.VersionTableDef;
import se.sics.hop.metadata.ndb.mysqlserver.MysqlServerConnector;

public class ClusterjConnector implements StorageConnector<Session> {

  private static ClusterjConnector instance;
  static SessionFactory sessionFactory;
  static ThreadLocal<Session> sessionPool = new ThreadLocal<Session>();
  static final Logger LOG = Logger.getLogger(ClusterjConnector.class);

  /*@Override
   public void clearSession() {
   Session session = obtainSession();
   if (session != null && !session.currentTransaction().isActive()) {
   LOG.info("Session object being closed.-" + session);

   session.flush();
   session.close();
   sessionPool.remove();
   }
   }*/
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
  public Session obtainSession() throws StorageException {
    try {
      Session session = sessionPool.get();
      System.out.println("obtainSession for thread " + Thread.currentThread().getId());
      if (session == null) {
        session = sessionFactory.getSession();
        LOG.info("New session object being obtained.-" + session);
        sessionPool.set(session);
      }
      return session;
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  /**
   * begin a transaction.
   */
  @Override
  public void beginTransaction(String name) throws StorageException {
    try {
      Session session = obtainSession();
      LOG.debug(name + " begin transaction for thread " + Thread.currentThread().getId());
      if (session.currentTransaction().isActive()) {
        LOG.error("Can not start Tx inside another Tx");
        System.exit(0);
      }
      session.currentTransaction().begin();
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  /**
   * Commit a transaction.
   */
  @Override
  public void commit() throws StorageException {
    try {
      Session session = obtainSession();
      Transaction tx = session.currentTransaction();
      if (!tx.isActive()) {
        throw new StorageException("The transaction is not began!");
      }

      tx.commit();
      session.flush();
      System.out.println("end transaction for thread " + Thread.currentThread().getId());
//    sessionPool.remove();
//    session.close();
    } catch (StorageException e){
      System.out.println("commit did not work");
      e.printStackTrace();
      throw new StorageException(e);
    }
  }

  /**
   * It rolls back only when the transaction is active.
   */
  @Override
  public void rollback() throws StorageException {
    try {
      Session session = obtainSession();
      Transaction tx = session.currentTransaction();
      if (tx.isActive()) {
        tx.rollback();
      }
    //sessionPool.remove();
      //session.close();
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  /**
   * This is called only when MiniDFSCluster wants to format the Namenode.
   */
  @Override
  public boolean formatStorage() throws StorageException {

    return formatStorage(/*INodeDataAccess.class, BlockInfoDataAccess.class,
             LeaseDataAccess.class, LeasePathDataAccess.class, ReplicaDataAccess.class,
             ReplicaUnderConstructionDataAccess.class, InvalidateBlockDataAccess.class,
             ExcessReplicaDataAccess.class, PendingBlockDataAccess.class, CorruptReplicaDataAccess.class,
             UnderReplicatedBlockDataAccess.class, LeaderDataAccess.class,
             INodeAttributesDataAccess.class, VariableDataAccess.class, StorageIdMapDataAccess.class, 
             BlockLookUpDataAccess.class*/VariableDataAccess.class, AppMasterRPCDataAccess.class,
            ApplicationStateDataAccess.class, ApplicationAttemptStateDataAccess.class, DelegationKeyDataAccess.class,
            DelegationTokenDataAccess.class, SequenceNumberDataAccess.class,
            RMStateVersionDataAccess.class, YarnVariablesDataAccess.class,
            AppSchedulingInfoDataAccess.class, AppSchedulingInfoBlacklistDataAccess.class,
            ContainerDataAccess.class,  ContainerIdToCleanDataAccess.class,
            ContainerStatusDataAccess.class, FiCaSchedulerAppLastScheduledContainerDataAccess.class,
            FiCaSchedulerAppLiveContainersDataAccess.class, FiCaSchedulerAppNewlyAllocatedContainersDataAccess.class,
            FiCaSchedulerAppReservationsDataAccess.class, FiCaSchedulerAppReservedContainersDataAccess.class,
            FiCaSchedulerAppSchedulingOpportunitiesDataAccess.class, FiCaSchedulerNodeDataAccess.class,
            FifoSchedulerNodesDataAccess.class,
            JustLaunchedContainersDataAccess.class, LaunchedContainersDataAccess.class,
            NodeDataAccess.class, QueueMetricsDataAccess.class,
            ResourceDataAccess.class, ResourceRequestDataAccess.class, RMContainerDataAccess.class,
            RMNodeDataAccess.class, SchedulerApplicationDataAccess.class, SequenceNumberDataAccess.class,
             FinishedApplicationsDataAccess.class,
            TokenDataAccess.class, RMContextInactiveNodesDataAccess.class, RMContextActiveNodesDataAccess.class,
            UpdatedContainerInfoDataAccess.class);
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
          } else if (e == StorageIdMapDataAccess.class) {
            MysqlServerConnector.truncateTable(StorageIdMapTableDef.TABLE_NAME);
          } else if (e == BlockLookUpDataAccess.class) {
            MysqlServerConnector.truncateTable(BlockLookUpTableDef.TABLE_NAME);
          } else if (e == AppMasterRPCDataAccess.class) {
            MysqlServerConnector.truncateTable(AppMasterRPCTableDef.TABLE_NAME);
          } else if (e == ApplicationStateDataAccess.class) {
            MysqlServerConnector.truncateTable(ApplicationStateTableDef.TABLE_NAME);
          } else if (e == ApplicationAttemptStateDataAccess.class) {
            MysqlServerConnector.truncateTable(ApplicationAttemptStateTableDef.TABLE_NAME);
          } else if (e == DelegationKeyDataAccess.class) {
            MysqlServerConnector.truncateTable(DelegationKeyTableDef.TABLE_NAME);
          } else if (e == DelegationTokenDataAccess.class) {
            MysqlServerConnector.truncateTable(DelegationTokenTableDef.TABLE_NAME);
          } else if (e == SequenceNumberDataAccess.class) {
            MysqlServerConnector.truncateTable(SequenceNumberTableDef.TABLE_NAME);
          } else if (e == RMStateVersionDataAccess.class) {
            MysqlServerConnector.truncateTable(VersionTableDef.TABLE_NAME);
          } else if (e == AppSchedulingInfoDataAccess.class) {
            truncate(AppSchedulingInfoTableDef.TABLE_NAME);
          } else if (e == AppSchedulingInfoBlacklistDataAccess.class) {
            truncate(AppSchedulingInfoBlacklistTableDef.TABLE_NAME);
          } else if (e == ContainerDataAccess.class) {
            truncate(ContainerTableDef.TABLE_NAME);
          } else if (e == ContainerIdToCleanDataAccess.class) {
            truncate(ContainerIdToCleanTableDef.TABLE_NAME);
          } else if (e == ContainerStatusDataAccess.class) {
            truncate(ContainerStatusTableDef.TABLE_NAME);
          } else if (e == FiCaSchedulerAppLastScheduledContainerDataAccess.class) {
            truncate(FiCaSchedulerAppLastScheduledContainerTableDef.TABLE_NAME);
          } else if (e == FiCaSchedulerAppLiveContainersDataAccess.class) {
            truncate(FiCaSchedulerAppLiveContainersTableDef.TABLE_NAME);
          } else if (e == FiCaSchedulerAppNewlyAllocatedContainersDataAccess.class) {
            truncate(FiCaSchedulerAppNewlyAllocatedContainersTableDef.TABLE_NAME);
          } else if (e == FiCaSchedulerAppReservationsDataAccess.class) {
            truncate(FiCaSchedulerAppReservationsTableDef.TABLE_NAME);
          } else if (e == FiCaSchedulerAppReservedContainersDataAccess.class) {
            truncate(FiCaSchedulerAppReservedContainersTableDef.TABLE_NAME);
          } else if (e == FiCaSchedulerAppSchedulingOpportunitiesDataAccess.class) {
            truncate(FiCaSchedulerAppSchedulingOpportunitiesTableDef.TABLE_NAME);
          } else if (e == FiCaSchedulerNodeDataAccess.class) {
            truncate(FiCaSchedulerNodeTableDef.TABLE_NAME);
          } else if (e == FifoSchedulerNodesDataAccess.class) {
            truncate(FifoSchedulerNodesTableDef.TABLE_NAME);
          } else if (e == JustLaunchedContainersDataAccess.class) {
            truncate(JustLaunchedContainersTableDef.TABLE_NAME);
          } else if (e == LaunchedContainersDataAccess.class) {
            truncate(LaunchedContainersTableDef.TABLE_NAME);
          } else if (e == NodeDataAccess.class) {
            truncate(NodeTableDef.TABLE_NAME);
          } else if (e == QueueMetricsDataAccess.class) {
            truncate(QueueMetricsTableDef.TABLE_NAME);
          } else if (e == ResourceDataAccess.class) {
            truncate(ResourceTableDef.TABLE_NAME);
          } else if (e == ResourceRequestDataAccess.class) {
            truncate(ResourceRequestTableDef.TABLE_NAME);
          } else if (e == RMContainerDataAccess.class) {
            truncate(RMContainerTableDef.TABLE_NAME);
          } else if (e == RMNodeDataAccess.class) {
            truncate(RMNodeTableDef.TABLE_NAME);
          } else if (e == SchedulerApplicationDataAccess.class) {
            truncate(SchedulerApplicationTableDef.TABLE_NAME);
          } else if (e == SequenceNumberDataAccess.class) {
            truncate(SequenceNumberTableDef.TABLE_NAME);
          } else if (e == FinishedApplicationsDataAccess.class) {
            truncate(FinishedApplicationsTableDef.TABLE_NAME);
          } else if (e == TokenDataAccess.class) {
            truncate(TokenTableDef.TABLE_NAME);
          } else if (e == RMContextInactiveNodesDataAccess.class) {
            truncate(RMContextInactiveNodesTableDef.TABLE_NAME);
          } else if (e == RMContextActiveNodesDataAccess.class) {
            truncate(RMContextActiveNodesTableDef.TABLE_NAME);
          } else if (e == UpdatedContainerInfoDataAccess.class) {
            truncate(UpdatedContainerInfoTableDef.TABLE_NAME);
          } else if (e == YarnVariablesDataAccess.class) {
            session.deletePersistentAll(YarnVariablesClusterJ.YarnVariablesDTO.class);
            for (int j = 0; j <= 18; j++) {
              YarnVariablesClusterJ.YarnVariablesDTO vd = session.newInstance(YarnVariablesClusterJ.YarnVariablesDTO.class);
              vd.setid(j);
              vd.setvalue(0);
              session.savePersistent(vd);
            }

          }
//          else if(e == StorageIdMapDataAccess.class){
//            MysqlServerConnector.truncateTable(StorageIdMapTableDef.TABLE_NAME);
//          }else if(e == BlockLookUpDataAccess.class){
//            MysqlServerConnector.truncateTable(BlockLookUpTableDef.TABLE_NAME);
//          }
        }
//        MysqlServerConnector.truncateTable("path_memcached");
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

  private void truncate(String tableName) throws StorageException, SQLException {
    MysqlServerConnector.truncateTable(tableName);
  }

  @Override
  public boolean isTransactionActive() throws StorageException {
    try {
      return obtainSession().currentTransaction().isActive();
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void stopStorage() throws StorageException {
  }

  @Override
  public void readLock() throws StorageException {
    try {
      Session session = obtainSession();
      session.setLockMode(LockMode.SHARED);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void writeLock() throws StorageException {
    try {
      Session session = obtainSession();
      session.setLockMode(LockMode.EXCLUSIVE);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void readCommitted() throws StorageException {
    try {
      Session session = obtainSession();
      session.setLockMode(LockMode.READ_COMMITTED);
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
      Session session = obtainSession();
      session.setPartitionKey(cls, key);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }
}
