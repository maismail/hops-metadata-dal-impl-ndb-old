package io.hops.metadata.ndb.dalimpl.yarn;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.hops.exception.StorageException;
import io.hops.metadata.yarn.entity.HopContainerId;
import io.hops.metadata.yarn.entity.HopFinishedApplications;
import io.hops.metadata.yarn.entity.HopJustLaunchedContainers;
import io.hops.metadata.yarn.entity.HopNextHeartbeat;
import io.hops.metadata.yarn.entity.HopNode;
import io.hops.metadata.yarn.entity.HopRMNodeComps;
import io.hops.metadata.yarn.entity.HopResource;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.dal.FullRMNodeDataAccess;
import io.hops.metadata.yarn.entity.HopNodeHBResponse;
import io.hops.metadata.yarn.entity.HopRMNode;
import io.hops.metadata.yarn.entity.HopUpdatedContainerInfo;
import io.hops.metadata.ndb.ClusterjConnector;

public class FullRMNodeClusterJ implements
    FullRMNodeDataAccess<HopRMNodeComps> {

  private final ClusterjConnector connector = ClusterjConnector.getInstance();
  private final JustLaunchedContainersClusterJ justLaunchedDA
          = new JustLaunchedContainersClusterJ();
  private final ResourceClusterJ resourceDA = new ResourceClusterJ();

  private final ContainerIdToCleanClusterJ containerToCleanDA
          = new ContainerIdToCleanClusterJ();
  private final FinishedApplicationsClusterJ finishedApplicationsDA
          = new FinishedApplicationsClusterJ();
  private final UpdatedContainerInfoClusterJ updatedContainerDA
          = new UpdatedContainerInfoClusterJ();

  @Override
  public HopRMNodeComps findByNodeId(String nodeId) throws StorageException {

    HopsSession session = connector.obtainSession();
    List<RMNodeComponentDTO> components = new ArrayList<RMNodeComponentDTO>();

    RMNodeClusterJ.RMNodeDTO rmnodeDTO = session.newInstance(
            RMNodeClusterJ.RMNodeDTO.class, nodeId);
    rmnodeDTO = session.load(rmnodeDTO);
    components.add(rmnodeDTO);

    NextHeartbeatClusterJ.NextHeartbeatDTO nextHBDTO = session.newInstance(
            NextHeartbeatClusterJ.NextHeartbeatDTO.class, nodeId);
    nextHBDTO = session.load(nextHBDTO);
    components.add(nextHBDTO);

    NodeClusterJ.NodeDTO nodeDTO = session.newInstance(
            NodeClusterJ.NodeDTO.class, nodeId);
    nodeDTO = session.load(nodeDTO);
    components.add(nodeDTO);

    NodeHBResponseClusterJ.NodeHBResponseDTO nodeHBResponseDTO = session.
            newInstance(NodeHBResponseClusterJ.NodeHBResponseDTO.class, nodeId);
    if (nodeHBResponseDTO != null) {
      nodeHBResponseDTO = session.load(nodeHBResponseDTO);
      components.add(nodeHBResponseDTO);
    }
    List<HopJustLaunchedContainers> hopJustLaunchedContainers = justLaunchedDA.
            findByRMNode(nodeId);
    List<ContainerStatusClusterJ.ContainerStatusDTO> containerStatusDTOs
            = new ArrayList<ContainerStatusClusterJ.ContainerStatusDTO>();
    if (hopJustLaunchedContainers != null) {
      for (HopJustLaunchedContainers hop : hopJustLaunchedContainers) {
        Object[] pk = new Object[]{hop.getContainerId(), hop.getRmnodeid()};
        ContainerStatusClusterJ.ContainerStatusDTO containerStatusDTO = session.
                newInstance(ContainerStatusClusterJ.ContainerStatusDTO.class, pk);
        containerStatusDTO = session.load(containerStatusDTO);
        containerStatusDTOs.add(containerStatusDTO);
      }
    }

    Map<Integer, List<HopUpdatedContainerInfo>> hopUpdatedContainerInfo
            = updatedContainerDA.findByRMNode(nodeId);
    if (hopUpdatedContainerInfo != null) {
      for (Integer uciId : hopUpdatedContainerInfo.keySet()) {
        for (HopUpdatedContainerInfo hop : hopUpdatedContainerInfo.get(uciId)) {
          Object[] pk = new Object[]{hop.getContainerId(), hop.getRmnodeid()};
          ContainerStatusClusterJ.ContainerStatusDTO containerStatusDTO
                  = session.
                  newInstance(ContainerStatusClusterJ.ContainerStatusDTO.class,
                          pk);
          containerStatusDTO = session.load(containerStatusDTO);
          containerStatusDTOs.add(containerStatusDTO);
        }
      }
    }
    HopResource hopResource = resourceDA.findEntry(nodeId,
            HopResource.TOTAL_CAPABILITY,
            HopResource.RMNODE);

    List<HopContainerId> hopContainerIdsToClean = containerToCleanDA.
            findByRMNode(nodeId);

    List<HopFinishedApplications> hopFinishedApplications
            = finishedApplicationsDA.findByRMNode(nodeId);

    HopRMNode hopRMNode = null;
    HopNode hopNode = null;
    HopNodeHBResponse hopNodeHBResponse = null;
    HopNextHeartbeat hopNextHeartbeat = null;
    for (RMNodeComponentDTO comp : components) {
      if (comp instanceof RMNodeClusterJ.RMNodeDTO) {
        hopRMNode = RMNodeClusterJ.createHopRMNode(
                (RMNodeClusterJ.RMNodeDTO) comp);
        //If commandport is zero, node was not found so return null
        //This is due to ClusterJ issue with returning a DTO object even if
        //the row was not found in the DB!
        if(hopRMNode.getHostName() == null){
          return null;
        }
      } else if (comp instanceof NodeClusterJ.NodeDTO) {
        hopNode = NodeClusterJ.
                createHopNode((NodeClusterJ.NodeDTO) comp);
      } else if (comp instanceof NodeHBResponseClusterJ.NodeHBResponseDTO) {
        hopNodeHBResponse = NodeHBResponseClusterJ.
                createHopNodeHBResponse(
                        (NodeHBResponseClusterJ.NodeHBResponseDTO) comp);
      } else if (comp instanceof NextHeartbeatClusterJ.NextHeartbeatDTO) {
        hopNextHeartbeat = NextHeartbeatClusterJ.createHopNextHeartbeat(
                (NextHeartbeatClusterJ.NextHeartbeatDTO) comp);
      }
    }

    return new HopRMNodeComps(hopRMNode, hopNextHeartbeat, hopNode,
            hopNodeHBResponse, hopResource, hopJustLaunchedContainers,
            hopUpdatedContainerInfo, hopContainerIdsToClean,
            hopFinishedApplications,
            ContainerStatusClusterJ.createMap(containerStatusDTOs));
  }

}
