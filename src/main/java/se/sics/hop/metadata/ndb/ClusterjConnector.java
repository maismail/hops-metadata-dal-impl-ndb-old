package se.sics.hop.metadata.ndb;

import com.mysql.clusterj.Constants;
import com.mysql.clusterj.LockMode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.sics.hop.StorageConnector;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.dal.BlockChecksumDataAccess;
import se.sics.hop.metadata.hdfs.dal.BlockInfoDataAccess;
import se.sics.hop.metadata.hdfs.dal.BlockLookUpDataAccess;
import se.sics.hop.metadata.hdfs.dal.CorruptReplicaDataAccess;
import se.sics.hop.metadata.hdfs.dal.EncodingStatusDataAccess;
import se.sics.hop.metadata.hdfs.dal.EntityDataAccess;
import se.sics.hop.metadata.hdfs.dal.ExcessReplicaDataAccess;
import se.sics.hop.metadata.hdfs.dal.INodeAttributesDataAccess;
import se.sics.hop.metadata.hdfs.dal.INodeDataAccess;
import se.sics.hop.metadata.hdfs.dal.InvalidateBlockDataAccess;
import se.sics.hop.metadata.hdfs.dal.LeaseDataAccess;
import se.sics.hop.metadata.hdfs.dal.LeasePathDataAccess;
import se.sics.hop.metadata.hdfs.dal.MisReplicatedRangeQueueDataAccess;
import se.sics.hop.metadata.hdfs.dal.PendingBlockDataAccess;
import se.sics.hop.metadata.hdfs.dal.QuotaUpdateDataAccess;
import se.sics.hop.metadata.hdfs.dal.ReplicaDataAccess;
import se.sics.hop.metadata.hdfs.dal.ReplicaUnderConstructionDataAccess;
import se.sics.hop.metadata.hdfs.dal.SafeBlocksDataAccess;
import se.sics.hop.metadata.hdfs.dal.StorageIdMapDataAccess;
import se.sics.hop.metadata.hdfs.dal.UnderReplicatedBlockDataAccess;
import se.sics.hop.metadata.hdfs.dal.VariableDataAccess;
import se.sics.hop.metadata.hdfs.entity.hop.var.HopVariable;
import se.sics.hop.metadata.hdfs.tabledef.BlockChecksumTableDef;
import se.sics.hop.metadata.hdfs.tabledef.BlockInfoTableDef;
import se.sics.hop.metadata.hdfs.tabledef.BlockLookUpTableDef;
import se.sics.hop.metadata.hdfs.tabledef.CorruptReplicaTableDef;
import se.sics.hop.metadata.hdfs.tabledef.EncodingStatusTableDef;
import se.sics.hop.metadata.hdfs.tabledef.ExcessReplicaTableDef;
import se.sics.hop.metadata.hdfs.tabledef.INodeAttributesTableDef;
import se.sics.hop.metadata.hdfs.tabledef.INodeTableDef;
import se.sics.hop.metadata.hdfs.tabledef.InvalidatedBlockTableDef;
import se.sics.hop.metadata.hdfs.tabledef.LeasePathTableDef;
import se.sics.hop.metadata.hdfs.tabledef.LeaseTableDef;
import se.sics.hop.metadata.hdfs.tabledef.MisReplicatedRangeQueueTableDef;
import se.sics.hop.metadata.hdfs.tabledef.PendingBlockTableDef;
import se.sics.hop.metadata.hdfs.tabledef.QuotaUpdateTableDef;
import se.sics.hop.metadata.hdfs.tabledef.ReplicaTableDef;
import se.sics.hop.metadata.hdfs.tabledef.ReplicaUnderConstructionTableDef;
import se.sics.hop.metadata.hdfs.tabledef.SafeBlocksTableDef;
import se.sics.hop.metadata.hdfs.tabledef.StorageIdMapTableDef;
import se.sics.hop.metadata.hdfs.tabledef.UnderReplicatedBlockTableDef;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.BlockChecksumClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.BlockInfoClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.CorruptReplicaClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.EncodingStatusClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.ExcessReplicaClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.HdfsLeaderClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.INodeAttributesClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.INodeClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.InvalidatedBlockClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.LeaseClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.LeasePathClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.PendingBlockClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.QuotaUpdateClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.ReplicaClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.ReplicaUnderConstructionClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.UnderReplicatedBlockClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.VariableClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.YarnLeaderClusterj;
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
import se.sics.hop.metadata.yarn.tabledef.rmstatestore.SequenceNumberTableDef;
import se.sics.hop.metadata.ndb.mysqlserver.HopsSQLExceptionHelper;
import se.sics.hop.metadata.ndb.mysqlserver.MysqlServerConnector;
import se.sics.hop.metadata.yarn.dal.rmstatestore.AllocateResponseDataAccess;
import se.sics.hop.metadata.yarn.dal.rmstatestore.SecretMamagerKeysDataAccess;
import se.sics.hop.metadata.yarn.tabledef.rmstatestore.AllocateResponseTableDef;
import se.sics.hop.metadata.yarn.tabledef.rmstatestore.SecretMamagerKeysTableDef;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.ndb.wrapper.HopsTransaction;

import java.sql.SQLException;
import java.util.Properties;
import se.sics.hop.metadata.hdfs.dal.HdfsLeaderDataAccess;
import se.sics.hop.metadata.hdfs.dal.YarnLeaderDataAccess;
import se.sics.hop.metadata.hdfs.tabledef.HdfsLeaderTableDef;
import se.sics.hop.metadata.hdfs.tabledef.YarnLeaderTableDef;

public class ClusterjConnector implements StorageConnector<DBSession> {

  private final static ClusterjConnector instance = new ClusterjConnector();
  private static boolean isInitialized = false;
  private DBSessionProvider dbSessionProvider = null;
  static ThreadLocal<DBSession> sessions = new ThreadLocal<DBSession>();
  static final Log LOG = LogFactory.getLog(ClusterjConnector.class);

  private ClusterjConnector() {
  }

  public static ClusterjConnector getInstance() {
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

    int initialPoolSize =
        Integer.parseInt((String) conf.get("se.kth.hop.session.pool.size"));
    int reuseCount =
        Integer.parseInt((String) conf.get("se.kth.hop.session.reuse.count"));
    dbSessionProvider =
        new DBSessionProvider(conf, reuseCount, initialPoolSize);

    isInitialized = true;
  }

  /*
   * Return a dbSession from a random dbSession factory in our pool.
   *
   * NOTE: Do not close the dbSession returned by this call or you will die.
   */
  @Override
  public HopsSession obtainSession() throws StorageException {
    DBSession dbSession = sessions.get();
    if (dbSession == null) {
      dbSession = dbSessionProvider.getSession();
      sessions.set(dbSession);
    }
    return dbSession.getSession();
  }

  private void returnSession(boolean error) throws StorageException {
    DBSession dbSession = sessions.get();
    sessions.remove(); // remove, and return to the pool
    dbSessionProvider.returnSession(dbSession, error); // if there was an error then close the session
  }

  /**
   * begin a transaction.
   */
  @Override
  public void beginTransaction(String name) throws StorageException {
    HopsSession session = obtainSession();
    LOG.debug(name + " begin transaction for thread " + Thread.currentThread().getId());
    if (session.currentTransaction().isActive()) {
      LOG.fatal("Prevented starting transaction within a transaction.");
      throw new Error("Can not start Tx inside another Tx");
    }
    session.currentTransaction().begin();
  }

  /**
   * Commit a transaction.
   */
  @Override
  public void commit() throws StorageException {
    HopsSession session = null;
    boolean dbError = false;
    try {
      session = obtainSession();
      HopsTransaction tx = session.currentTransaction();
      if (!tx.isActive()) {
        throw new StorageException("The transaction is not began!");
      }
      tx.commit();
    } catch (StorageException e) {
      dbError = true;
      throw e;
    } finally {
      returnSession(dbError);
    }
  }

  /**
   * It rolls back only when the transaction is active.
   */
  @Override
  public void rollback() throws StorageException {
    HopsSession session = null;
    boolean dbError = false;
    try {
      session = obtainSession();
      HopsTransaction tx = session.currentTransaction();
      if (tx.isActive()) {
        tx.rollback();
      }
    } catch (StorageException e) {
      dbError = true;
      throw e;
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
    return obtainSession().currentTransaction().isActive();
  }

  @Override
  public void stopStorage() throws StorageException {
    dbSessionProvider.stop();
  }

  @Override
  public void readLock() throws StorageException {
    HopsSession session = obtainSession();
    session.setLockMode(LockMode.SHARED);
  }

  @Override
  public void writeLock() throws StorageException {
    HopsSession session = obtainSession();
    session.setLockMode(LockMode.EXCLUSIVE);
  }

  @Override
  public void readCommitted() throws StorageException {
    HopsSession session = obtainSession();
    session.setLockMode(LockMode.READ_COMMITTED);
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
    } else if (className == HdfsLeaderDataAccess.class) {
      cls = HdfsLeaderClusterj.HdfsLeaderDTO.class;
    } else if (className == YarnLeaderDataAccess.class) {
      cls = YarnLeaderClusterj.YarnLeaderDTO.class;
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
    } else if (className == QuotaUpdateDataAccess.class) {
      cls = QuotaUpdateClusterj.QuotaUpdateDTO.class;
    } else if (className == EncodingStatusDataAccess.class) {
      cls = EncodingStatusClusterj.EncodingStatusDto.class;
    } else if (className == BlockChecksumDataAccess.class) {
      cls = BlockChecksumClusterj.BlockChecksumDto.class;
    }

    HopsSession session = obtainSession();
    session.setPartitionKey(cls, key);
    session.flush();
  }

  @Override
  public boolean formatStorageNonTransactional() throws StorageException {
    return format(false);
  }

  private boolean format(boolean transactional) throws StorageException {
    return format(transactional, VariableDataAccess.class, 
//            INodeDataAccess.class, BlockInfoDataAccess.class,
//            LeaseDataAccess.class, LeasePathDataAccess.class, ReplicaDataAccess.class,
//            ReplicaUnderConstructionDataAccess.class, InvalidateBlockDataAccess.class,
//            ExcessReplicaDataAccess.class, PendingBlockDataAccess.class, CorruptReplicaDataAccess.class,
//            UnderReplicatedBlockDataAccess.class, HdfsLeaderDataAccess.class,
//            INodeAttributesDataAccess.class, StorageIdMapDataAccess.class,
//            BlockLookUpDataAccess.class, SafeBlocksDataAccess.class, MisReplicatedRangeQueueDataAccess.class,
//            QuotaUpdateDataAccess.class, EncodingStatusDataAccess.class, BlockChecksumDataAccess.class,
            
            AppMasterRPCDataAccess.class,
            ApplicationStateDataAccess.class, ApplicationAttemptStateDataAccess.class, DelegationKeyDataAccess.class,
            DelegationTokenDataAccess.class, SequenceNumberDataAccess.class,
            RMStateVersionDataAccess.class, YarnVariablesDataAccess.class,
            AppSchedulingInfoDataAccess.class, AppSchedulingInfoBlacklistDataAccess.class,
            ContainerDataAccess.class,  ContainerIdToCleanDataAccess.class,
            ContainerStatusDataAccess.class, FiCaSchedulerAppLastScheduledContainerDataAccess.class,
            FiCaSchedulerAppLiveContainersDataAccess.class, FiCaSchedulerAppNewlyAllocatedContainersDataAccess.class,
            FiCaSchedulerAppReservationsDataAccess.class, FiCaSchedulerAppReservedContainersDataAccess.class,
            FiCaSchedulerAppSchedulingOpportunitiesDataAccess.class, FiCaSchedulerNodeDataAccess.class,
            JustLaunchedContainersDataAccess.class, LaunchedContainersDataAccess.class,
            NodeDataAccess.class, QueueMetricsDataAccess.class,
            ResourceDataAccess.class, ResourceRequestDataAccess.class, RMContainerDataAccess.class,
            RMNodeDataAccess.class, SchedulerApplicationDataAccess.class, SequenceNumberDataAccess.class,
             FinishedApplicationsDataAccess.class,
            TokenDataAccess.class, RMContextInactiveNodesDataAccess.class, RMContextActiveNodesDataAccess.class,
            UpdatedContainerInfoDataAccess.class, YarnLeaderDataAccess.class, SecretMamagerKeysDataAccess.class,
            AllocateResponseDataAccess.class
            );
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
          } else if (e == HdfsLeaderDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional,HdfsLeaderTableDef.TABLE_NAME);
          } else if (e == YarnLeaderDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional,YarnLeaderTableDef.TABLE_NAME);
          } else if (e == INodeAttributesDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional, INodeAttributesTableDef.TABLE_NAME);
          } else if (e == VariableDataAccess.class) {
            HopsSession session = obtainSession();
            session.currentTransaction().begin();
            session.deletePersistentAll(VariableClusterj.VariableDTO.class);
            for (HopVariable.Finder varType : HopVariable.Finder.values()) {
              VariableClusterj.VariableDTO vd = session.newInstance(
                  VariableClusterj.VariableDTO.class);
              vd.setId(varType.getId());
              vd.setValue(varType.getDefaultValue());
              session.savePersistent(vd);
            }
            session.currentTransaction().commit();
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
          } else if (e == EncodingStatusDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional, EncodingStatusTableDef.TABLE_NAME);
          } else if (e == BlockChecksumDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional, BlockChecksumTableDef.TABLE_NAME);
          } else if (e == AppSchedulingInfoDataAccess.class) {
            truncate(transactional,AppSchedulingInfoTableDef.TABLE_NAME);
          } else if (e == AppSchedulingInfoBlacklistDataAccess.class) {
            truncate(transactional,AppSchedulingInfoBlacklistTableDef.TABLE_NAME);
          } else if (e == ContainerDataAccess.class) {
            truncate(transactional,ContainerTableDef.TABLE_NAME);
          } else if (e == ContainerIdToCleanDataAccess.class) {
            truncate(transactional,ContainerIdToCleanTableDef.TABLE_NAME);
          } else if (e == ContainerStatusDataAccess.class) {
            truncate(transactional,ContainerStatusTableDef.TABLE_NAME);
          } else if (e == FiCaSchedulerAppLastScheduledContainerDataAccess.class) {
            truncate(transactional,FiCaSchedulerAppLastScheduledContainerTableDef.TABLE_NAME);
          } else if (e == FiCaSchedulerAppLiveContainersDataAccess.class) {
            truncate(transactional,FiCaSchedulerAppLiveContainersTableDef.TABLE_NAME);
          } else if (e == FiCaSchedulerAppNewlyAllocatedContainersDataAccess.class) {
            truncate(transactional,FiCaSchedulerAppNewlyAllocatedContainersTableDef.TABLE_NAME);
          } else if (e == FiCaSchedulerAppReservationsDataAccess.class) {
            truncate(transactional,FiCaSchedulerAppReservationsTableDef.TABLE_NAME);
          } else if (e == FiCaSchedulerAppReservedContainersDataAccess.class) {
            truncate(transactional,FiCaSchedulerAppReservedContainersTableDef.TABLE_NAME);
          } else if (e == FiCaSchedulerAppSchedulingOpportunitiesDataAccess.class) {
            truncate(transactional,FiCaSchedulerAppSchedulingOpportunitiesTableDef.TABLE_NAME);
          } else if (e == FiCaSchedulerNodeDataAccess.class) {
            truncate(transactional,FiCaSchedulerNodeTableDef.TABLE_NAME);
          } else if (e == JustLaunchedContainersDataAccess.class) {
            truncate(transactional,JustLaunchedContainersTableDef.TABLE_NAME);
          } else if (e == LaunchedContainersDataAccess.class) {
            truncate(transactional,LaunchedContainersTableDef.TABLE_NAME);
          } else if (e == NodeDataAccess.class) {
            truncate(transactional,NodeTableDef.TABLE_NAME);
          } else if (e == QueueMetricsDataAccess.class) {
            truncate(transactional,QueueMetricsTableDef.TABLE_NAME);
          } else if (e == ResourceDataAccess.class) {
            truncate(transactional,ResourceTableDef.TABLE_NAME);
          } else if (e == ResourceRequestDataAccess.class) {
            truncate(transactional,ResourceRequestTableDef.TABLE_NAME);
          } else if (e == RMContainerDataAccess.class) {
            truncate(transactional,RMContainerTableDef.TABLE_NAME);
          } else if (e == RMNodeDataAccess.class) {
            truncate(transactional,RMNodeTableDef.TABLE_NAME);
          } else if (e == SchedulerApplicationDataAccess.class) {
            truncate(transactional,SchedulerApplicationTableDef.TABLE_NAME);
          } else if (e == SequenceNumberDataAccess.class) {
            truncate(transactional,SequenceNumberTableDef.TABLE_NAME);
          } else if (e == FinishedApplicationsDataAccess.class) {
            truncate(transactional,FinishedApplicationsTableDef.TABLE_NAME);
          } else if (e == TokenDataAccess.class) {
            truncate(transactional,TokenTableDef.TABLE_NAME);
          } else if (e == RMContextInactiveNodesDataAccess.class) {
            truncate(transactional,RMContextInactiveNodesTableDef.TABLE_NAME);
          } else if (e == RMContextActiveNodesDataAccess.class) {
            truncate(transactional,RMContextActiveNodesTableDef.TABLE_NAME);
          } else if (e == UpdatedContainerInfoDataAccess.class) {
            truncate(transactional,UpdatedContainerInfoTableDef.TABLE_NAME);
          } else if (e == SecretMamagerKeysDataAccess.class){
            truncate(transactional,SecretMamagerKeysTableDef.TABLE_NAME);
          } else if (e == AllocateResponseDataAccess.class){
            truncate(transactional,AllocateResponseTableDef.TABLE_NAME);
          } else if (e == YarnVariablesDataAccess.class) {
            HopsSession session = obtainSession();
            session.currentTransaction().begin();
            session.deletePersistentAll(YarnVariablesClusterJ.YarnVariablesDTO.class);
            for (int j = 0; j <= 18; j++) {
              YarnVariablesClusterJ.YarnVariablesDTO vd = session.newInstance(YarnVariablesClusterJ.YarnVariablesDTO.class);
              vd.setid(j);
              vd.setvalue(0);
              session.savePersistent(vd);
            }

          }
        }
        MysqlServerConnector.truncateTable(transactional, "path_memcached");
        return true;

      } catch (SQLException ex) {
        LOG.error(ex.getMessage(), ex);
        throw HopsSQLExceptionHelper.wrap(ex);
      }
    } // end retry loop
    return false;
  }
  
   private void truncate(boolean transactional, String tableName) throws StorageException, SQLException {
    MysqlServerConnector.truncateTable(transactional,tableName);
  }
}
