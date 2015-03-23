package io.hops.metadata.ndb.dalimpl.yarn;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.hops.exception.StorageException;
import io.hops.metadata.yarn.entity.HopContainerId;
import io.hops.metadata.yarn.entity.HopContainerStatus;
import io.hops.metadata.yarn.entity.HopFinishedApplications;
import io.hops.metadata.yarn.entity.HopJustLaunchedContainers;
import io.hops.metadata.yarn.entity.HopNode;
import io.hops.metadata.yarn.entity.HopRMNodeComps;
import io.hops.metadata.yarn.entity.HopResource;
import io.hops.metadata.ndb.NdbStorageFactory;
import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.dal.FullRMNodeDataAccess;
import io.hops.metadata.yarn.dal.NodeDataAccess;
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.dal.ResourceDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.handler.RequestHandler;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import io.hops.StorageConnector;
import io.hops.metadata.yarn.entity.HopNodeHBResponse;
import io.hops.metadata.yarn.entity.HopRMNode;
import io.hops.metadata.yarn.entity.HopUpdatedContainerInfo;
import io.hops.metadata.yarn.dal.ContainerIdToCleanDataAccess;
import io.hops.metadata.yarn.dal.FinishedApplicationsDataAccess;
import io.hops.metadata.yarn.dal.JustLaunchedContainersDataAccess;
import io.hops.metadata.yarn.dal.NodeHBResponseDataAccess;
import io.hops.metadata.yarn.dal.UpdatedContainerInfoDataAccess;
import io.hops.metadata.yarn.tabledef.ContainerStatusTableDef;

public class TestFullRMNodeClusterJ {

  NdbStorageFactory storageFactory = new NdbStorageFactory();
  StorageConnector connector = storageFactory.getConnector();

  @Before
  public void setup() throws IOException {
    storageFactory.setConfiguration(getMetadataClusterConfiguration());
    RequestHandler.setStorageConnector(connector);
    LightWeightRequestHandler setRMDTMasterKeyHandler
            = new LightWeightRequestHandler(YARNOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                connector.formatStorage();

                return null;
              }
            };
    setRMDTMasterKeyHandler.handle();

  }

  @Test
  public void testFindByNodeId() throws StorageException, IOException {
    //TODO: Add test for nextheartbeat
    //fill the database with a RMNode
    final HopRMNode hopRMNodeOrigin = new HopRMNode("70", "rmnode70",
            9999, 9876, "127.0.0.1",
            "hop.sics.se", "life is good ", -10L, "relax",
            "hayarn",
            10, 3);
    final HopNode hopNodeOrigin = new HopNode("70", "rmnode70", "ici",
            1000, "papa");
    final HopNodeHBResponse hopNHBROrigin = new HopNodeHBResponse("70",
            new byte[]{new Integer(1).byteValue()});
    final HopResource hopResourceOrigin = new HopResource("70",
            HopResource.TOTAL_CAPABILITY, HopResource.RMNODE, 1,
            100);

    final List<HopJustLaunchedContainers> hopJustLaunchedContainers
            = new ArrayList<HopJustLaunchedContainers>();
    hopJustLaunchedContainers.add(
            new HopJustLaunchedContainers("70", "container1"));
    hopJustLaunchedContainers.add(
            new HopJustLaunchedContainers("70", "container2"));

    final List<HopUpdatedContainerInfo> hopUpdatedContainers
            = new ArrayList<HopUpdatedContainerInfo>();
    hopUpdatedContainers.add(new HopUpdatedContainerInfo("70",
            "container3", 1));
    hopUpdatedContainers.add(new HopUpdatedContainerInfo("70",
            "container4", 2));

    final List<HopContainerId> hopContainerIds
            = new ArrayList<HopContainerId>();
    hopContainerIds.add(new HopContainerId("70", "container5"));
    hopContainerIds.add(new HopContainerId("70", "container6"));

    final List<HopFinishedApplications> hopFinishedApps
            = new ArrayList<HopFinishedApplications>();
    hopFinishedApps.add(new HopFinishedApplications("70", "app1"));
    hopFinishedApps.add(new HopFinishedApplications("70", "app2"));

    final List<HopContainerStatus> hopContainersStatus
            = new ArrayList<HopContainerStatus>();
    hopContainersStatus.add(new HopContainerStatus("container1",
            ContainerStatusTableDef.STATE_RUNNING, "every thing is good", 0,
            "70"));
    hopContainersStatus.add(new HopContainerStatus("container2",
            ContainerStatusTableDef.STATE_RUNNING, "every thing is good", 0,
            "70"));
    hopContainersStatus.add(new HopContainerStatus("container3",
            ContainerStatusTableDef.STATE_RUNNING, "every thing is good", 0,
            "70"));
    hopContainersStatus.add(new HopContainerStatus("container4",
            ContainerStatusTableDef.STATE_RUNNING, "every thing is good", 0,
            "70"));
    hopContainersStatus.add(new HopContainerStatus("container5",
            ContainerStatusTableDef.STATE_COMPLETED, "every thing is good", 0,
            "70"));
    hopContainersStatus.add(new HopContainerStatus("container6",
            ContainerStatusTableDef.STATE_COMPLETED, "finish", 1, "70"));

    LightWeightRequestHandler fillDB
            = new LightWeightRequestHandler(YARNOperationType.TEST) {
              @Override
              public Object performTask() throws StorageException {
                connector.beginTransaction();
                connector.writeLock();

                RMNodeDataAccess rmNodeDA = (RMNodeDataAccess) storageFactory.
                getDataAccess(RMNodeDataAccess.class);
                rmNodeDA.add(hopRMNodeOrigin);

                NodeDataAccess nodeDA = (NodeDataAccess) storageFactory.
                getDataAccess(
                        NodeDataAccess.class);
                nodeDA.createNode(hopNodeOrigin);

                NodeHBResponseDataAccess nodeHBRDA
                = (NodeHBResponseDataAccess) storageFactory.getDataAccess(
                        NodeHBResponseDataAccess.class);
                nodeHBRDA.add(hopNHBROrigin);

                ResourceDataAccess resourceDA
                = (ResourceDataAccess) storageFactory.getDataAccess(
                        ResourceDataAccess.class);

                resourceDA.add(hopResourceOrigin);

                JustLaunchedContainersDataAccess justLaunchedContainerDA
                = (JustLaunchedContainersDataAccess) storageFactory.
                getDataAccess(
                        JustLaunchedContainersDataAccess.class);
                justLaunchedContainerDA.addAll(hopJustLaunchedContainers);

                UpdatedContainerInfoDataAccess updatedContainerDA
                = (UpdatedContainerInfoDataAccess) storageFactory.getDataAccess(
                        UpdatedContainerInfoDataAccess.class);
                updatedContainerDA.addAll(hopUpdatedContainers);

                ContainerIdToCleanDataAccess containersIdDA
                = (ContainerIdToCleanDataAccess) storageFactory.getDataAccess(
                        ContainerIdToCleanDataAccess.class);
                containersIdDA.addAll(hopContainerIds);

                FinishedApplicationsDataAccess finishedAppDA
                = (FinishedApplicationsDataAccess) storageFactory.getDataAccess(
                        FinishedApplicationsDataAccess.class);
                finishedAppDA.addAll(hopFinishedApps);

                ContainerStatusDataAccess containerStatusDA
                = (ContainerStatusDataAccess) storageFactory.getDataAccess(
                        ContainerStatusDataAccess.class);
                containerStatusDA.addAll(hopContainersStatus);
                connector.commit();
                return null;
              }
            };
    fillDB.handle();

    //get the RMNode
    LightWeightRequestHandler getHopRMNode
            = new LightWeightRequestHandler(YARNOperationType.TEST) {
              @Override
              public Object performTask() throws StorageException {
                connector.beginTransaction();
                connector.writeLock();
                FullRMNodeDataAccess fullRMNodeDA
                = (FullRMNodeDataAccess) storageFactory.
                getDataAccess(FullRMNodeDataAccess.class);
                HopRMNodeComps hopRMNodeFull = (HopRMNodeComps) fullRMNodeDA.
                findByNodeId("70");
                connector.commit();
                return hopRMNodeFull;
              }
            };

    HopRMNodeComps hopRMNodeFull = (HopRMNodeComps) getHopRMNode.handle();

    //check if the fetched RMNode is correct
    HopRMNode rmNodeFinal = hopRMNodeFull.getHopRMNode();
    Assert.assertTrue(rmNodeFinal.getNodeId().equals(hopRMNodeOrigin.
            getNodeId()));
    Assert.assertTrue(rmNodeFinal.getCurrentState().equals(hopRMNodeOrigin.
            getCurrentState()));

    HopNode nodeFinal = hopRMNodeFull.getHopNode();
    Assert.assertTrue(nodeFinal.getLevel() == hopNodeOrigin.getLevel());

    HopNodeHBResponse nodeHBRFinal = hopRMNodeFull.getHopNodeHBResponse();
    Assert.assertTrue(nodeHBRFinal.getResponse()[0] == (hopNHBROrigin.
            getResponse()[0]));

    HopResource resourceFinal = hopRMNodeFull.getHopResource();
    Assert.assertTrue(resourceFinal.getParent() == hopResourceOrigin.
            getParent());

    List<HopJustLaunchedContainers> hopJustLaunchedContainersFinal
            = hopRMNodeFull.getHopJustLaunchedContainers();
    for (HopJustLaunchedContainers justLaunched : hopJustLaunchedContainersFinal) {
      Assert.assertTrue(hopRMNodeFull.getHopContainersStatus().containsKey(
              justLaunched.getContainerId()));
      boolean flag = false;
      for (HopJustLaunchedContainers justLaunchedOringin
              : hopJustLaunchedContainers) {
        if (justLaunchedOringin.getContainerId().equals(justLaunched.
                getContainerId())) {
          flag = true;
          break;
        }
      }
      Assert.assertTrue(flag);
    }

    Map<Integer, List<HopUpdatedContainerInfo>> hopUpdatedContainersFinal
            = hopRMNodeFull.getHopUpdatedContainerInfo();
    for (Integer uciId : hopUpdatedContainersFinal.keySet()) {
      for (HopUpdatedContainerInfo updated : hopUpdatedContainersFinal.
              get(uciId)) {
        Assert.assertTrue(hopRMNodeFull.getHopContainersStatus().containsKey(
                updated.getContainerId()));
        boolean flag = false;
        for (HopUpdatedContainerInfo updatedOringin : hopUpdatedContainers) {
          if (updated.getContainerId().equals(updatedOringin.getContainerId())) {
            flag = true;
            break;
          }
        }
        Assert.assertTrue(flag);
      }
    }
    List<HopContainerId> hopContainerIdsFinal = hopRMNodeFull.
            getHopContainerIdsToClean();
    for (HopContainerId updated : hopContainerIdsFinal) {
      boolean flag = false;
      for (HopContainerId containerIdOringine : hopContainerIds) {
        if (updated.getContainerId().
                equals(containerIdOringine.getContainerId())) {
          flag = true;
          break;
        }
      }
      Assert.assertTrue(flag);
    }

    List<HopFinishedApplications> hopFinishedAppsFinal = hopRMNodeFull.
            getHopFinishedApplications();
    for (HopFinishedApplications finishedApp : hopFinishedAppsFinal) {
      boolean flag = false;
      for (HopFinishedApplications finishedAppOringine : hopFinishedApps) {
        if (finishedApp.getApplicationId().equals(finishedAppOringine.
                getApplicationId())) {
          flag = true;
          break;
        }
      }
      Assert.assertTrue(flag);
    }

    Collection<HopContainerStatus> hopContainersStatusFinal = hopRMNodeFull.
            getHopContainersStatus().values();
    for (HopContainerStatus containerStatus : hopContainersStatusFinal) {
      boolean flag = false;
      for (HopContainerStatus containerStatusOringine : hopContainersStatus) {
        if (containerStatus.getContainerid().equals(containerStatusOringine.
                getContainerid()) && containerStatus.getExitstatus()
                == containerStatusOringine.getExitstatus()) {
          flag = true;
          break;
        }
      }
      Assert.assertTrue(flag);
    }
  }

  private Properties getMetadataClusterConfiguration()
          throws IOException {
    String configFile = "ndb-config.properties";
    Properties clusterConf = new Properties();
    InputStream inStream = StorageConnector.class.getClassLoader().
            getResourceAsStream(configFile);
    clusterConf.load(inStream);
    return clusterConf;
  }
}
