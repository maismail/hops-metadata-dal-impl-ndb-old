package io.hops.metadata.ndb;

import com.mysql.clusterj.Constants;
import com.mysql.clusterj.LockMode;
import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.dal.BlockInfoDataAccess;
import io.hops.metadata.hdfs.dal.CorruptReplicaDataAccess;
import io.hops.metadata.hdfs.dal.EncodingStatusDataAccess;
import io.hops.metadata.hdfs.dal.ExcessReplicaDataAccess;
import io.hops.metadata.election.dal.HdfsLeDescriptorDataAccess;
import io.hops.metadata.hdfs.dal.INodeAttributesDataAccess;
import io.hops.metadata.hdfs.dal.InvalidateBlockDataAccess;
import io.hops.metadata.hdfs.dal.LeasePathDataAccess;
import io.hops.metadata.hdfs.dal.MisReplicatedRangeQueueDataAccess;
import io.hops.metadata.hdfs.dal.PendingBlockDataAccess;
import io.hops.metadata.hdfs.dal.QuotaUpdateDataAccess;
import io.hops.metadata.hdfs.dal.ReplicaDataAccess;
import io.hops.metadata.hdfs.dal.SafeBlocksDataAccess;
import io.hops.metadata.hdfs.dal.StorageIdMapDataAccess;
import io.hops.metadata.hdfs.dal.UnderReplicatedBlockDataAccess;
import io.hops.metadata.hdfs.dal.VariableDataAccess;
import io.hops.metadata.common.entity.Variable;
import io.hops.metadata.hdfs.tabledef.BlockChecksumTableDef;
import io.hops.metadata.hdfs.tabledef.BlockInfoTableDef;
import io.hops.metadata.hdfs.tabledef.BlockLookUpTableDef;
import io.hops.metadata.hdfs.tabledef.ExcessReplicaTableDef;
import io.hops.metadata.election.tabledef.HdfsLeaderTableDef;
import io.hops.metadata.hdfs.tabledef.INodeAttributesTableDef;
import io.hops.metadata.hdfs.tabledef.InvalidatedBlockTableDef;
import io.hops.metadata.hdfs.tabledef.LeasePathTableDef;
import io.hops.metadata.hdfs.tabledef.LeaseTableDef;
import io.hops.metadata.hdfs.tabledef.MisReplicatedRangeQueueTableDef;
import io.hops.metadata.hdfs.tabledef.PendingBlockTableDef;
import io.hops.metadata.hdfs.tabledef.ReplicaTableDef;
import io.hops.metadata.hdfs.tabledef.ReplicaUnderConstructionTableDef;
import io.hops.metadata.hdfs.tabledef.SafeBlocksTableDef;
import io.hops.metadata.hdfs.tabledef.StorageIdMapTableDef;
import io.hops.metadata.hdfs.tabledef.UnderReplicatedBlockTableDef;
import io.hops.metadata.election.tabledef.YarnLeaderTableDef;
import io.hops.metadata.ndb.dalimpl.hdfs.BlockChecksumClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.BlockInfoClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.CorruptReplicaClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.EncodingStatusClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.ExcessReplicaClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.HdfsLeaderClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.INodeAttributesClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.INodeClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.LeaseClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.LeasePathClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.PendingBlockClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.QuotaUpdateClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.ReplicaClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.UnderReplicatedBlockClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.VariableClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.YarnLeaderClusterj;
import io.hops.metadata.ndb.dalimpl.yarn.YarnVariablesClusterJ;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.ndb.wrapper.HopsTransaction;
import io.hops.metadata.yarn.dal.AppSchedulingInfoBlacklistDataAccess;
import io.hops.metadata.yarn.dal.ContainerDataAccess;
import io.hops.metadata.yarn.dal.ContainerIdToCleanDataAccess;
import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppLiveContainersDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppNewlyAllocatedContainersDataAccess;
import io.hops.metadata.yarn.dal.JustLaunchedContainersDataAccess;
import io.hops.metadata.yarn.dal.LaunchedContainersDataAccess;
import io.hops.metadata.yarn.dal.NextHeartbeatDataAccess;
import io.hops.metadata.yarn.dal.NodeDataAccess;
import io.hops.metadata.yarn.dal.QueueMetricsDataAccess;
import io.hops.metadata.yarn.dal.RMContextActiveNodesDataAccess;
import io.hops.metadata.yarn.dal.RMContextInactiveNodesDataAccess;
import io.hops.metadata.yarn.dal.RMLoadDataAccess;
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.dal.ResourceDataAccess;
import io.hops.metadata.yarn.dal.capacity.FiCaSchedulerAppLastScheduledContainerDataAccess;
import io.hops.metadata.yarn.dal.capacity.FiCaSchedulerAppReservedContainersDataAccess;
import io.hops.metadata.yarn.dal.capacity.FiCaSchedulerAppSchedulingOpportunitiesDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.AllocateResponseDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationAttemptStateDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.DelegationTokenDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.RMStateVersionDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.SecretMamagerKeysDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.SequenceNumberDataAccess;
import io.hops.metadata.yarn.tabledef.AppSchedulingInfoBlacklistTableDef;
import io.hops.metadata.yarn.tabledef.AppSchedulingInfoTableDef;
import io.hops.metadata.yarn.tabledef.ContainerIdToCleanTableDef;
import io.hops.metadata.yarn.tabledef.ContainerTableDef;
import io.hops.metadata.yarn.tabledef.FiCaSchedulerAppLiveContainersTableDef;
import io.hops.metadata.yarn.tabledef.FiCaSchedulerNodeTableDef;
import io.hops.metadata.yarn.tabledef.FinishedApplicationsTableDef;
import io.hops.metadata.yarn.tabledef.LaunchedContainersTableDef;
import io.hops.metadata.yarn.tabledef.NextHeartbeatTableDef;
import io.hops.metadata.yarn.tabledef.NodeTableDef;
import io.hops.metadata.yarn.tabledef.PendingEventTableDef;
import io.hops.metadata.yarn.tabledef.QueueMetricsTableDef;
import io.hops.metadata.yarn.tabledef.RMContainerTableDef;
import io.hops.metadata.yarn.tabledef.RMContextActiveNodesTableDef;
import io.hops.metadata.yarn.tabledef.RMContextInactiveNodesTableDef;
import io.hops.metadata.yarn.tabledef.RMLoadTableDef;
import io.hops.metadata.yarn.tabledef.RMNodeTableDef;
import io.hops.metadata.yarn.tabledef.ResourceRequestTableDef;
import io.hops.metadata.yarn.tabledef.SchedulerApplicationTableDef;
import io.hops.metadata.yarn.tabledef.UpdatedContainerInfoTableDef;
import io.hops.metadata.yarn.tabledef.appmasterrpc.RPCTableDef;
import io.hops.metadata.yarn.tabledef.capacity.FiCaSchedulerAppLastScheduledContainerTableDef;
import io.hops.metadata.yarn.tabledef.capacity.FiCaSchedulerAppReservationsTableDef;
import io.hops.metadata.yarn.tabledef.capacity.FiCaSchedulerAppReservedContainersTableDef;
import io.hops.metadata.yarn.tabledef.capacity.FiCaSchedulerAppSchedulingOpportunitiesTableDef;
import io.hops.metadata.yarn.tabledef.rmstatestore.AllocateResponseTableDef;
import io.hops.metadata.yarn.tabledef.rmstatestore.ApplicationAttemptStateTableDef;
import io.hops.metadata.yarn.tabledef.rmstatestore.DelegationKeyTableDef;
import io.hops.metadata.yarn.tabledef.rmstatestore.SecretMamagerKeysTableDef;
import io.hops.metadata.yarn.tabledef.rmstatestore.SequenceNumberTableDef;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import io.hops.metadata.hdfs.dal.BlockChecksumDataAccess;
import io.hops.metadata.hdfs.dal.BlockLookUpDataAccess;
import io.hops.metadata.common.EntityDataAccess;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.metadata.hdfs.dal.LeaseDataAccess;
import io.hops.metadata.hdfs.dal.ReplicaUnderConstructionDataAccess;
import io.hops.metadata.hdfs.tabledef.CorruptReplicaTableDef;
import io.hops.metadata.hdfs.tabledef.EncodingStatusTableDef;
import io.hops.metadata.hdfs.tabledef.INodeTableDef;
import io.hops.metadata.hdfs.tabledef.QuotaUpdateTableDef;
import io.hops.metadata.ndb.dalimpl.hdfs.InvalidatedBlockClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.ReplicaUnderConstructionClusterj;
import io.hops.metadata.yarn.dal.AppSchedulingInfoDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.RPCDataAccess;
import io.hops.metadata.yarn.dal.capacity.FiCaSchedulerAppReservationsDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerNodeDataAccess;
import io.hops.metadata.yarn.dal.FinishedApplicationsDataAccess;
import io.hops.metadata.yarn.dal.RMContainerDataAccess;
import io.hops.metadata.yarn.dal.ResourceRequestDataAccess;
import io.hops.metadata.yarn.dal.SchedulerApplicationDataAccess;
import io.hops.metadata.yarn.dal.UpdatedContainerInfoDataAccess;
import io.hops.metadata.yarn.dal.YarnVariablesDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.DelegationKeyDataAccess;
import io.hops.metadata.yarn.tabledef.ContainerStatusTableDef;
import io.hops.metadata.yarn.tabledef.FiCaSchedulerAppNewlyAllocatedContainersTableDef;
import io.hops.metadata.yarn.tabledef.JustLaunchedContainersTableDef;
import io.hops.metadata.yarn.tabledef.ResourceTableDef;
import io.hops.metadata.ndb.mysqlserver.MysqlServerConnector;
import io.hops.metadata.yarn.dal.PendingEventDataAccess;

import java.sql.SQLException;
import java.util.Properties;

import io.hops.metadata.election.dal.YarnLeDescriptorDataAccess;
import io.hops.metadata.yarn.tabledef.rmstatestore.ApplicationStateTableDef;
import io.hops.metadata.yarn.tabledef.rmstatestore.DelegationTokenTableDef;
import io.hops.metadata.yarn.tabledef.rmstatestore.RMStateVersionTableDef;

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
        Integer.parseInt((String) conf.get("io.hops.session.pool.size"));
    int reuseCount =
        Integer.parseInt((String) conf.get("io.hops.session.reuse.count"));
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
   * @param name
   * @throws io.hops.exception.StorageException
   */
  @Override
  public void beginTransaction() throws StorageException {
    HopsSession session = obtainSession();
    if (session.currentTransaction().isActive()) {
      LOG.fatal("Prevented starting transaction within a transaction.");
      throw new Error("Can not start Tx inside another Tx");
    }
    session.currentTransaction().begin();
  }

  /**
   * Commit a transaction.
   * @throws io.hops.exception.StorageException
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
    } else if (className == HdfsLeDescriptorDataAccess.class) {
      cls = HdfsLeaderClusterj.HdfsLeaderDTO.class;
    } else if (className == YarnLeDescriptorDataAccess.class) {
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
    return format(transactional,
        // shared
        VariableDataAccess.class,
        // HDFS
        INodeDataAccess.class, BlockInfoDataAccess.class,
        LeaseDataAccess.class, LeasePathDataAccess.class, ReplicaDataAccess.class,
        ReplicaUnderConstructionDataAccess.class, InvalidateBlockDataAccess.class,
        ExcessReplicaDataAccess.class, PendingBlockDataAccess.class, CorruptReplicaDataAccess.class,
        UnderReplicatedBlockDataAccess.class, HdfsLeDescriptorDataAccess.class,
        INodeAttributesDataAccess.class, StorageIdMapDataAccess.class,
        BlockLookUpDataAccess.class, SafeBlocksDataAccess.class, MisReplicatedRangeQueueDataAccess.class,
        QuotaUpdateDataAccess.class, EncodingStatusDataAccess.class, BlockChecksumDataAccess.class,
        // YARN
        RPCDataAccess.class,
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
         FinishedApplicationsDataAccess.class, RMContextInactiveNodesDataAccess.class, RMContextActiveNodesDataAccess.class,
        UpdatedContainerInfoDataAccess.class, YarnLeDescriptorDataAccess.class,
        SecretMamagerKeysDataAccess.class, AllocateResponseDataAccess.class, RMLoadDataAccess.class);
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
          } else if (e == HdfsLeDescriptorDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional, HdfsLeaderTableDef.TABLE_NAME);
          } else if (e == INodeAttributesDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional, INodeAttributesTableDef.TABLE_NAME);
          } else if (e == VariableDataAccess.class) {
            HopsSession session = obtainSession();
            session.currentTransaction().begin();
            session.deletePersistentAll(VariableClusterj.VariableDTO.class);
            for (Variable.Finder varType : Variable.Finder.values()) {
              VariableClusterj.VariableDTO vd = session.newInstance(
                  VariableClusterj.VariableDTO.class);
              vd.setId(varType.getId());
              vd.setValue(varType.getDefaultValue());
              session.savePersistent(vd);
            }
            session.currentTransaction().commit();
          } else if (e == StorageIdMapDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional,
                StorageIdMapTableDef.TABLE_NAME);
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
          } else if (e == YarnLeDescriptorDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional, YarnLeaderTableDef.TABLE_NAME);
          } else if (e == AppSchedulingInfoDataAccess.class) {
            truncate(transactional, AppSchedulingInfoTableDef.TABLE_NAME);
          } else if (e == AppSchedulingInfoBlacklistDataAccess.class) {
            truncate(transactional, AppSchedulingInfoBlacklistTableDef.TABLE_NAME);
          } else if (e == ContainerDataAccess.class) {
            truncate(transactional, ContainerTableDef.TABLE_NAME);
          } else if (e == ContainerIdToCleanDataAccess.class) {
            truncate(transactional, ContainerIdToCleanTableDef.TABLE_NAME);
          } else if (e == ContainerStatusDataAccess.class) {
            truncate(transactional,ContainerStatusTableDef.TABLE_NAME);
          } else if (e == FiCaSchedulerAppLastScheduledContainerDataAccess.class) {
            truncate(transactional,
                FiCaSchedulerAppLastScheduledContainerTableDef.TABLE_NAME);
          } else if (e == FiCaSchedulerAppLiveContainersDataAccess.class) {
            truncate(transactional, FiCaSchedulerAppLiveContainersTableDef.TABLE_NAME);
          } else if (e == FiCaSchedulerAppNewlyAllocatedContainersDataAccess.class) {
            truncate(transactional,FiCaSchedulerAppNewlyAllocatedContainersTableDef.TABLE_NAME);
          } else if (e == FiCaSchedulerAppReservationsDataAccess.class) {
            truncate(transactional, FiCaSchedulerAppReservationsTableDef.TABLE_NAME);
          } else if (e == FiCaSchedulerAppReservedContainersDataAccess.class) {
            truncate(transactional, FiCaSchedulerAppReservedContainersTableDef.TABLE_NAME);
          } else if (e == FiCaSchedulerAppSchedulingOpportunitiesDataAccess.class) {
            truncate(transactional,
                FiCaSchedulerAppSchedulingOpportunitiesTableDef.TABLE_NAME);
          } else if (e == FiCaSchedulerNodeDataAccess.class) {
            truncate(transactional, FiCaSchedulerNodeTableDef.TABLE_NAME);
          } else if (e == JustLaunchedContainersDataAccess.class) {
            truncate(transactional,JustLaunchedContainersTableDef.TABLE_NAME);
          } else if (e == LaunchedContainersDataAccess.class) {
            truncate(transactional, LaunchedContainersTableDef.TABLE_NAME);
          } else if (e == NodeDataAccess.class) {
            truncate(transactional, NodeTableDef.TABLE_NAME);
          } else if (e == QueueMetricsDataAccess.class) {
            truncate(transactional, QueueMetricsTableDef.TABLE_NAME);
          } else if (e == ResourceDataAccess.class) {
            truncate(transactional,ResourceTableDef.TABLE_NAME);
          } else if (e == ResourceRequestDataAccess.class) {
            truncate(transactional, ResourceRequestTableDef.TABLE_NAME);
          } else if (e == RMContainerDataAccess.class) {
            truncate(transactional, RMContainerTableDef.TABLE_NAME);
          } else if (e == RMNodeDataAccess.class) {
            // Truncate does not work with foreign keys
            truncate(true, RMNodeTableDef.TABLE_NAME);
          } else if (e == SchedulerApplicationDataAccess.class) {
            truncate(transactional, SchedulerApplicationTableDef.TABLE_NAME);
          } else if (e == SequenceNumberDataAccess.class) {
            truncate(transactional, SequenceNumberTableDef.TABLE_NAME);
          } else if (e == FinishedApplicationsDataAccess.class) {
            truncate(transactional, FinishedApplicationsTableDef.TABLE_NAME);
          } else if (e == RMContextInactiveNodesDataAccess.class) {
            truncate(transactional, RMContextInactiveNodesTableDef.TABLE_NAME);
          } else if (e == RMContextActiveNodesDataAccess.class) {
            truncate(transactional, RMContextActiveNodesTableDef.TABLE_NAME);
          } else if (e == UpdatedContainerInfoDataAccess.class) {
            truncate(transactional, UpdatedContainerInfoTableDef.TABLE_NAME);
          } else if (e == SecretMamagerKeysDataAccess.class){
            truncate(transactional, SecretMamagerKeysTableDef.TABLE_NAME);
          } else if (e == AllocateResponseDataAccess.class){
            truncate(transactional, AllocateResponseTableDef.TABLE_NAME);
          } else if (e == DelegationKeyDataAccess.class){
            truncate(transactional, DelegationKeyTableDef.TABLE_NAME);
          } else if (e== DelegationTokenDataAccess.class){
            truncate(transactional, DelegationTokenTableDef.TABLE_NAME);
          } else if (e == RMStateVersionDataAccess.class){
            truncate(transactional, RMStateVersionTableDef.TABLE_NAME);
          } else if (e == ApplicationAttemptStateDataAccess.class){
            truncate(transactional, ApplicationAttemptStateTableDef.TABLE_NAME);
          } else if (e == ApplicationStateDataAccess.class){
            truncate(transactional, ApplicationStateTableDef.TABLE_NAME);
          } else if (e == RPCDataAccess.class){
            truncate(transactional, RPCTableDef.TABLE_NAME);
          } else if (e == RMLoadDataAccess.class){
            truncate(transactional, RMLoadTableDef.TABLE_NAME);
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
            session.currentTransaction().commit();
          }
          else if (e == PendingEventDataAccess.class){
            truncate(transactional, PendingEventTableDef.TABLE_NAME);
          } else if (e == NextHeartbeatDataAccess.class){
            truncate(transactional, NextHeartbeatTableDef.TABLE_NAME);
          }
        }
        MysqlServerConnector.truncateTable(transactional, "path_memcached");
        return true;

      } catch (SQLException ex) {
        LOG.error(ex.getMessage(), ex);
      }
    } // end retry loop
    return false;
  }
  
   private void truncate(boolean transactional, String tableName) throws StorageException, SQLException {
    MysqlServerConnector.truncateTable(transactional,tableName);
  }
}
