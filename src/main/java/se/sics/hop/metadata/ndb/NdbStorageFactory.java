package se.sics.hop.metadata.ndb;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import se.sics.hop.DALStorageFactory;
import se.sics.hop.StorageConnector;
import se.sics.hop.metadata.hdfs.dal.*;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.*;
import se.sics.hop.exception.StorageInitializtionException;
import se.sics.hop.metadata.hdfs.dal.BlockInfoDataAccess;
import se.sics.hop.metadata.hdfs.dal.BlockLookUpDataAccess;
import se.sics.hop.metadata.hdfs.dal.CorruptReplicaDataAccess;
import se.sics.hop.metadata.hdfs.dal.EntityDataAccess;
import se.sics.hop.metadata.hdfs.dal.ExcessReplicaDataAccess;
import se.sics.hop.metadata.hdfs.dal.INodeAttributesDataAccess;
import se.sics.hop.metadata.hdfs.dal.INodeDataAccess;
import se.sics.hop.metadata.hdfs.dal.InvalidateBlockDataAccess;
import se.sics.hop.metadata.hdfs.dal.LeaseDataAccess;
import se.sics.hop.metadata.hdfs.dal.LeasePathDataAccess;
import se.sics.hop.metadata.hdfs.dal.PendingBlockDataAccess;
import se.sics.hop.metadata.hdfs.dal.ReplicaDataAccess;
import se.sics.hop.metadata.hdfs.dal.ReplicaUnderConstructionDataAccess;
import se.sics.hop.metadata.hdfs.dal.StorageIdMapDataAccess;
import se.sics.hop.metadata.hdfs.dal.UnderReplicatedBlockDataAccess;
import se.sics.hop.metadata.hdfs.dal.VariableDataAccess;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.BlockInfoClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.BlockLookUpClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.CorruptReplicaClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.ExcessReplicaClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.INodeAttributesClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.INodeClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.InvalidatedBlockClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.LeaseClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.LeasePathClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.PendingBlockClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.ReplicaClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.ReplicaUnderConstructionClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.StorageIdMapClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.UnderReplicatedBlockClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.VariableClusterj;
import se.sics.hop.metadata.ndb.dalimpl.yarn.AppSchedulingInfoBlacklistClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.AppSchedulingInfoClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.ContainerClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.ContainerIdToCleanClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.ContainerStatusClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.capacity.FiCaSchedulerAppLastScheduledContainerClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.FiCaSchedulerAppLiveContainersClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.FiCaSchedulerAppNewlyAllocatedContainersClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.capacity.FiCaSchedulerAppReservationsClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.capacity.FiCaSchedulerAppReservedContainersClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.capacity.FiCaSchedulerAppSchedulingOpportunitiesClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.FiCaSchedulerNodeClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.FinishedApplicationsClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.JustLaunchedContainersClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.LaunchedContainersClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.NodeClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.NodeHBResponseClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.QueueMetricsClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.RMContainerClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.RMContextInactiveNodesClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.RMContextActiveNodesClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.RMNodeClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.ResourceClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.ResourceRequestClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.SchedulerApplicationClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.UpdatedContainerInfoClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.YarnVariablesClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.fair.FSSchedulerNodeClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.rmstatestore.AllocateResponseClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.rmstatestore.AppMasterRPCClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.rmstatestore.ApplicationAttemptStateClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.rmstatestore.ApplicationStateClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.rmstatestore.DelegationKeyClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.rmstatestore.DelegationTokenClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.rmstatestore.RMStateVersionClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.rmstatestore.SequenceNumberClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.rmstatestore.SecretMamagerKeysClusterJ;
import se.sics.hop.metadata.ndb.mysqlserver.MysqlServerConnector;
import se.sics.hop.metadata.yarn.dal.AppSchedulingInfoBlacklistDataAccess;
import se.sics.hop.metadata.yarn.dal.AppSchedulingInfoDataAccess;
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
import se.sics.hop.metadata.yarn.dal.NodeHBResponseDataAccess;
import se.sics.hop.metadata.yarn.dal.QueueMetricsDataAccess;
import se.sics.hop.metadata.yarn.dal.RMContainerDataAccess;
import se.sics.hop.metadata.yarn.dal.RMContextInactiveNodesDataAccess;
import se.sics.hop.metadata.yarn.dal.RMContextActiveNodesDataAccess;
import se.sics.hop.metadata.yarn.dal.RMNodeDataAccess;
import se.sics.hop.metadata.yarn.dal.ResourceDataAccess;
import se.sics.hop.metadata.yarn.dal.ResourceRequestDataAccess;
import se.sics.hop.metadata.yarn.dal.SchedulerApplicationDataAccess;
import se.sics.hop.metadata.yarn.dal.UpdatedContainerInfoDataAccess;
import se.sics.hop.metadata.yarn.dal.YarnVariablesDataAccess;
import se.sics.hop.metadata.yarn.dal.fair.FSSchedulerNodeDataAccess;
import se.sics.hop.metadata.yarn.dal.rmstatestore.AllocateResponseDataAccess;
import se.sics.hop.metadata.yarn.dal.rmstatestore.AppMasterRPCDataAccess;
import se.sics.hop.metadata.yarn.dal.rmstatestore.ApplicationAttemptStateDataAccess;
import se.sics.hop.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import se.sics.hop.metadata.yarn.dal.rmstatestore.DelegationKeyDataAccess;
import se.sics.hop.metadata.yarn.dal.rmstatestore.DelegationTokenDataAccess;
import se.sics.hop.metadata.yarn.dal.rmstatestore.RMStateVersionDataAccess;
import se.sics.hop.metadata.yarn.dal.rmstatestore.SecretMamagerKeysDataAccess;
import se.sics.hop.metadata.yarn.dal.rmstatestore.SequenceNumberDataAccess;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class NdbStorageFactory implements DALStorageFactory {

  private Map<Class, EntityDataAccess> dataAccessMap = new HashMap<Class, EntityDataAccess>();

  @Override
  public void setConfiguration(String configFile) throws StorageInitializtionException {
    try {
      Properties conf = new Properties();
      InputStream inStream = this.getClass().getClassLoader().getResourceAsStream(configFile);
      conf.load(inStream);
      ClusterjConnector.getInstance().setConfiguration(conf);
      MysqlServerConnector.getInstance().setConfiguration(conf);
      initDataAccessMap();
    } catch (IOException ex) {
      throw new StorageInitializtionException(ex);
    }
  }

  private void initDataAccessMap() {
    //RM STATE STORE
    dataAccessMap.put(RMStateVersionDataAccess.class, new RMStateVersionClusterJ());
    dataAccessMap.put(ApplicationStateDataAccess.class, new ApplicationStateClusterJ());
    dataAccessMap.put(ApplicationAttemptStateDataAccess.class, new ApplicationAttemptStateClusterJ());
    dataAccessMap.put(DelegationTokenDataAccess.class, new DelegationTokenClusterJ());
    dataAccessMap.put(SequenceNumberDataAccess.class, new SequenceNumberClusterJ());
    dataAccessMap.put(DelegationKeyDataAccess.class, new DelegationKeyClusterJ());
    dataAccessMap.put(YarnVariablesDataAccess.class, new YarnVariablesClusterJ());
    dataAccessMap.put(AppMasterRPCDataAccess.class, new AppMasterRPCClusterJ());
    dataAccessMap.put(QueueMetricsDataAccess.class, new QueueMetricsClusterJ());
    dataAccessMap.put(FiCaSchedulerNodeDataAccess.class, new FiCaSchedulerNodeClusterJ());
    dataAccessMap.put(ResourceDataAccess.class, new ResourceClusterJ());
    dataAccessMap.put(NodeDataAccess.class, new NodeClusterJ());
    dataAccessMap.put(ResourceDataAccess.class, new ResourceClusterJ());
    dataAccessMap.put(RMNodeDataAccess.class, new RMNodeClusterJ());
    dataAccessMap.put(RMContextActiveNodesDataAccess.class, new RMContextActiveNodesClusterJ());
    dataAccessMap.put(RMContextInactiveNodesDataAccess.class, new RMContextInactiveNodesClusterJ());
    dataAccessMap.put(ContainerStatusDataAccess.class, new ContainerStatusClusterJ());
    dataAccessMap.put(NodeHBResponseDataAccess.class, new NodeHBResponseClusterJ());
    dataAccessMap.put(UpdatedContainerInfoDataAccess.class, new UpdatedContainerInfoClusterJ());
    dataAccessMap.put(ContainerIdToCleanDataAccess.class, new ContainerIdToCleanClusterJ());
    dataAccessMap.put(JustLaunchedContainersDataAccess.class, new JustLaunchedContainersClusterJ());
    dataAccessMap.put(LaunchedContainersDataAccess.class, new LaunchedContainersClusterJ());
    dataAccessMap.put(FinishedApplicationsDataAccess.class, new FinishedApplicationsClusterJ());
    dataAccessMap.put(SchedulerApplicationDataAccess.class, new SchedulerApplicationClusterJ());
    dataAccessMap.put(FiCaSchedulerAppNewlyAllocatedContainersDataAccess.class, new FiCaSchedulerAppNewlyAllocatedContainersClusterJ());
    dataAccessMap.put(FiCaSchedulerAppSchedulingOpportunitiesDataAccess.class, new FiCaSchedulerAppSchedulingOpportunitiesClusterJ());
    dataAccessMap.put(FiCaSchedulerAppLastScheduledContainerDataAccess.class, new FiCaSchedulerAppLastScheduledContainerClusterJ());
    dataAccessMap.put(FiCaSchedulerAppLiveContainersDataAccess.class, new FiCaSchedulerAppLiveContainersClusterJ());
    dataAccessMap.put(FiCaSchedulerAppReservedContainersDataAccess.class, new FiCaSchedulerAppReservedContainersClusterJ());
    dataAccessMap.put(FiCaSchedulerAppReservationsDataAccess.class, new FiCaSchedulerAppReservationsClusterJ());
    dataAccessMap.put(RMContainerDataAccess.class, new RMContainerClusterJ());
    dataAccessMap.put(ContainerDataAccess.class, new ContainerClusterJ());
    dataAccessMap.put(AppSchedulingInfoDataAccess.class, new AppSchedulingInfoClusterJ());
    dataAccessMap.put(AppSchedulingInfoBlacklistDataAccess.class, new AppSchedulingInfoBlacklistClusterJ());
    dataAccessMap.put(ResourceRequestDataAccess.class, new ResourceRequestClusterJ());
    dataAccessMap.put(BlockInfoDataAccess.class, new BlockInfoClusterj());
    dataAccessMap.put(PendingBlockDataAccess.class, new PendingBlockClusterj());
    dataAccessMap.put(ReplicaUnderConstructionDataAccess.class, new ReplicaUnderConstructionClusterj());
    dataAccessMap.put(INodeDataAccess.class, new INodeClusterj());
    dataAccessMap.put(INodeAttributesDataAccess.class, new INodeAttributesClusterj());
    dataAccessMap.put(LeaseDataAccess.class, new LeaseClusterj());
    dataAccessMap.put(LeasePathDataAccess.class, new LeasePathClusterj());
    dataAccessMap.put(HdfsLeaderDataAccess.class, new HdfsLeaderClusterj());
    dataAccessMap.put(YarnLeaderDataAccess.class, new YarnLeaderClusterj());
    dataAccessMap.put(ReplicaDataAccess.class, new ReplicaClusterj());
    dataAccessMap.put(CorruptReplicaDataAccess.class, new CorruptReplicaClusterj());
    dataAccessMap.put(ExcessReplicaDataAccess.class, new ExcessReplicaClusterj());
    dataAccessMap.put(InvalidateBlockDataAccess.class, new InvalidatedBlockClusterj());
    dataAccessMap.put(UnderReplicatedBlockDataAccess.class, new UnderReplicatedBlockClusterj());
    dataAccessMap.put(VariableDataAccess.class, new VariableClusterj());
    dataAccessMap.put(StorageIdMapDataAccess.class, new StorageIdMapClusterj());
    dataAccessMap.put(BlockLookUpDataAccess.class, new BlockLookUpClusterj());
    
    dataAccessMap.put(FSSchedulerNodeDataAccess.class, new FSSchedulerNodeClusterJ());
    dataAccessMap.put(QuotaUpdateDataAccess.class, new QuotaUpdateClusterj());
    dataAccessMap.put(SecretMamagerKeysDataAccess.class, new SecretMamagerKeysClusterJ());
    dataAccessMap.put(AllocateResponseDataAccess.class, new AllocateResponseClusterJ());
  }

  @Override
  public StorageConnector getConnector() {
    return ClusterjConnector.getInstance();
  }

  @Override
  public EntityDataAccess getDataAccess(Class type) {
    return dataAccessMap.get(type);
  }
}
