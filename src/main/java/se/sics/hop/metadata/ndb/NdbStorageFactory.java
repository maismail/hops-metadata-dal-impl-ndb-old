package se.sics.hop.metadata.ndb;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import se.sics.hop.DALStorageFactory;
import se.sics.hop.StorageConnector;
import se.sics.hop.exception.StorageInitializtionException;

import se.sics.hop.metadata.hdfs.dal.EntityDataAccess;
import se.sics.hop.metadata.ndb.dalimpl.yarn.AppSchedulingInfoClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.AppSchedulingInfoPrioritiesClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.AppSchedulingInfoRequestsClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.ApplicationAttemptIdClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.ApplicationIdClusterJ;

import se.sics.hop.metadata.ndb.dalimpl.yarn.FiCaSchedulerNodeClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.QueueMetricsClusterJ;

import se.sics.hop.metadata.ndb.dalimpl.yarn.ContainerIdClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.ContainerIdToCleanClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.ContainerStatusClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.FiCaSchedulerAppClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.FiCaSchedulerAppLastScheduledContainerClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.FiCaSchedulerAppLiveContainersClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.FiCaSchedulerAppNewlyAllocatedContainersClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.FiCaSchedulerAppReservationsClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.FiCaSchedulerAppReservedContainersClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.FiCaSchedulerAppSchedulingOpportunitiesClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.FifoSchedulerAppsClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.FifoSchedulerNodesClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.FinishedApplicationsClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.JustLaunchedContainersClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.LaunchedContainersClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.NodeClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.NodeHBResponseClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.NodeIdClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.RMContainerClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.RMContextInactiveNodesClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.RMContextNodesClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.RMNodeClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.ResourceClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.ResourceRequestClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.SchedulerApplicationClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.UpdatedContainerInfoClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.UpdatedContainerInfoContainersClusterJ;

import se.sics.hop.metadata.ndb.dalimpl.yarn.YarnVariablesClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.rmstatestore.AppMasterRPCClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.rmstatestore.ApplicationAttemptStateClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.rmstatestore.ApplicationStateClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.rmstatestore.DelegationKeyClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.rmstatestore.DelegationTokenClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.rmstatestore.RMStateVersionClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.rmstatestore.SequenceNumberClusterJ;
import se.sics.hop.metadata.yarn.dal.rmstatestore.RMStateVersionDataAccess;
import se.sics.hop.metadata.ndb.mysqlserver.MysqlServerConnector;

import se.sics.hop.metadata.yarn.dal.AppSchedulingInfoDataAccess;
import se.sics.hop.metadata.yarn.dal.AppSchedulingInfoPrioritiesDataAccess;
import se.sics.hop.metadata.yarn.dal.AppSchedulingInfoRequestsDataAccess;
import se.sics.hop.metadata.yarn.dal.ApplicationAttemptIdDataAccess;
import se.sics.hop.metadata.yarn.dal.ApplicationIdDataAccess;
import se.sics.hop.metadata.yarn.dal.FiCaSchedulerNodeDataAccess;
import se.sics.hop.metadata.yarn.dal.QueueMetricsDataAccess;
import se.sics.hop.metadata.yarn.dal.ContainerIdDataAccess;
import se.sics.hop.metadata.yarn.dal.ContainerIdToCleanDataAccess;
import se.sics.hop.metadata.yarn.dal.ContainerStatusDataAccess;
import se.sics.hop.metadata.yarn.dal.FiCaSchedulerAppDataAccess;
import se.sics.hop.metadata.yarn.dal.FiCaSchedulerAppLastScheduledContainerDataAccess;
import se.sics.hop.metadata.yarn.dal.FiCaSchedulerAppLiveContainersDataAccess;
import se.sics.hop.metadata.yarn.dal.FiCaSchedulerAppNewlyAllocatedContainersDataAccess;
import se.sics.hop.metadata.yarn.dal.FiCaSchedulerAppReservationsDataAccess;
import se.sics.hop.metadata.yarn.dal.FiCaSchedulerAppReservedContainersDataAccess;
import se.sics.hop.metadata.yarn.dal.FiCaSchedulerAppSchedulingOpportunitiesDataAccess;
import se.sics.hop.metadata.yarn.dal.FifoSchedulerAppsDataAccess;
import se.sics.hop.metadata.yarn.dal.FifoSchedulerNodesDataAccess;
import se.sics.hop.metadata.yarn.dal.FinishedApplicationsDataAccess;
import se.sics.hop.metadata.yarn.dal.JustLaunchedContainersDataAccess;
import se.sics.hop.metadata.yarn.dal.LaunchedContainersDataAccess;
import se.sics.hop.metadata.yarn.dal.NodeDataAccess;
import se.sics.hop.metadata.yarn.dal.NodeHBResponseDataAccess;
import se.sics.hop.metadata.yarn.dal.NodeIdDataAccess;
import se.sics.hop.metadata.yarn.dal.RMContainerDataAccess;
import se.sics.hop.metadata.yarn.dal.RMContextInactiveNodesDataAccess;
import se.sics.hop.metadata.yarn.dal.RMContextNodesDataAccess;
import se.sics.hop.metadata.yarn.dal.RMNodeDataAccess;
import se.sics.hop.metadata.yarn.dal.ResourceDataAccess;
import se.sics.hop.metadata.yarn.dal.ResourceRequestDataAccess;
import se.sics.hop.metadata.yarn.dal.SchedulerApplicationDataAccess;
import se.sics.hop.metadata.yarn.dal.UpdatedContainerInfoContainersDataAccess;
import se.sics.hop.metadata.yarn.dal.UpdatedContainerInfoDataAccess;
import se.sics.hop.metadata.yarn.dal.YarnVariablesDataAccess;
import se.sics.hop.metadata.yarn.dal.rmstatestore.AppMasterRPCDataAccess;
import se.sics.hop.metadata.yarn.dal.rmstatestore.ApplicationAttemptStateDataAccess;
import se.sics.hop.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import se.sics.hop.metadata.yarn.dal.rmstatestore.DelegationKeyDataAccess;
import se.sics.hop.metadata.yarn.dal.rmstatestore.DelegationTokenDataAccess;
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
        dataAccessMap.put(ApplicationIdDataAccess.class, new ApplicationIdClusterJ());
        dataAccessMap.put(QueueMetricsDataAccess.class, new QueueMetricsClusterJ());
        dataAccessMap.put(FiCaSchedulerNodeDataAccess.class, new FiCaSchedulerNodeClusterJ());
        dataAccessMap.put(ResourceDataAccess.class, new ResourceClusterJ());
        dataAccessMap.put(NodeIdDataAccess.class, new NodeIdClusterJ());
        dataAccessMap.put(ApplicationAttemptIdDataAccess.class, new ApplicationAttemptIdClusterJ());
        dataAccessMap.put(NodeIdDataAccess.class, new NodeIdClusterJ());
        dataAccessMap.put(NodeDataAccess.class, new NodeClusterJ());
        dataAccessMap.put(ResourceDataAccess.class, new ResourceClusterJ());
        dataAccessMap.put(RMNodeDataAccess.class, new RMNodeClusterJ());
        dataAccessMap.put(RMContextNodesDataAccess.class, new RMContextNodesClusterJ());
        dataAccessMap.put(RMContextInactiveNodesDataAccess.class, new RMContextInactiveNodesClusterJ());
        dataAccessMap.put(ContainerStatusDataAccess.class, new ContainerStatusClusterJ());
        dataAccessMap.put(ContainerIdDataAccess.class, new ContainerIdClusterJ());
        dataAccessMap.put(NodeHBResponseDataAccess.class, new NodeHBResponseClusterJ());
        dataAccessMap.put(UpdatedContainerInfoDataAccess.class, new UpdatedContainerInfoClusterJ());
        dataAccessMap.put(UpdatedContainerInfoContainersDataAccess.class, new UpdatedContainerInfoContainersClusterJ());
        dataAccessMap.put(ContainerIdToCleanDataAccess.class, new ContainerIdToCleanClusterJ());
        dataAccessMap.put(JustLaunchedContainersDataAccess.class, new JustLaunchedContainersClusterJ());
        dataAccessMap.put(LaunchedContainersDataAccess.class, new LaunchedContainersClusterJ());
        dataAccessMap.put(FinishedApplicationsDataAccess.class, new FinishedApplicationsClusterJ());
        dataAccessMap.put(FifoSchedulerNodesDataAccess.class, new FifoSchedulerNodesClusterJ());
        dataAccessMap.put(FifoSchedulerAppsDataAccess.class, new FifoSchedulerAppsClusterJ());
        dataAccessMap.put(FiCaSchedulerAppDataAccess.class, new FiCaSchedulerAppClusterJ());
        dataAccessMap.put(SchedulerApplicationDataAccess.class, new SchedulerApplicationClusterJ());
        dataAccessMap.put(FiCaSchedulerAppNewlyAllocatedContainersDataAccess.class, new FiCaSchedulerAppNewlyAllocatedContainersClusterJ());
        dataAccessMap.put(FiCaSchedulerAppSchedulingOpportunitiesDataAccess.class, new FiCaSchedulerAppSchedulingOpportunitiesClusterJ());
        dataAccessMap.put(FiCaSchedulerAppLastScheduledContainerDataAccess.class, new FiCaSchedulerAppLastScheduledContainerClusterJ());
        dataAccessMap.put(FiCaSchedulerAppLiveContainersDataAccess.class, new FiCaSchedulerAppLiveContainersClusterJ());
        dataAccessMap.put(FiCaSchedulerAppReservedContainersDataAccess.class, new FiCaSchedulerAppReservedContainersClusterJ());
        dataAccessMap.put(FiCaSchedulerAppReservationsDataAccess.class, new FiCaSchedulerAppReservationsClusterJ());
        dataAccessMap.put(RMContainerDataAccess.class, new RMContainerClusterJ());
        dataAccessMap.put(AppSchedulingInfoDataAccess.class, new AppSchedulingInfoClusterJ());
        dataAccessMap.put(AppSchedulingInfoPrioritiesDataAccess.class, new AppSchedulingInfoPrioritiesClusterJ());
        dataAccessMap.put(AppSchedulingInfoRequestsDataAccess.class, new AppSchedulingInfoRequestsClusterJ());
        dataAccessMap.put(ResourceRequestDataAccess.class, new ResourceRequestClusterJ());
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
