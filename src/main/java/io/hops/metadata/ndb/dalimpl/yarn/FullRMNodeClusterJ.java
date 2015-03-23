package io.hops.metadata.ndb.dalimpl.yarn;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.hops.exception.StorageException;
import io.hops.metadata.yarn.entity.ContainerId;
import io.hops.metadata.yarn.entity.FinishedApplications;
import io.hops.metadata.yarn.entity.JustLaunchedContainers;
import io.hops.metadata.yarn.entity.NextHeartbeat;
import io.hops.metadata.yarn.entity.Node;
import io.hops.metadata.yarn.entity.RMNodeComps;
import io.hops.metadata.yarn.entity.Resource;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.dal.FullRMNodeDataAccess;
import io.hops.metadata.yarn.entity.NodeHBResponse;
import io.hops.metadata.yarn.entity.RMNode;
import io.hops.metadata.yarn.entity.UpdatedContainerInfo;
import io.hops.metadata.ndb.ClusterjConnector;

public class FullRMNodeClusterJ implements
    FullRMNodeDataAccess<RMNodeComps> {

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
  public RMNodeComps findByNodeId(String nodeId) throws StorageException {

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
    List<JustLaunchedContainers> hopJustLaunchedContainers = justLaunchedDA.
            findByRMNode(nodeId);
    List<ContainerStatusClusterJ.ContainerStatusDTO> containerStatusDTOs
            = new ArrayList<ContainerStatusClusterJ.ContainerStatusDTO>();
    if (hopJustLaunchedContainers != null) {
      for (JustLaunchedContainers hop : hopJustLaunchedContainers) {
        Object[] pk = new Object[]{hop.getContainerId(), hop.getRmnodeid()};
        ContainerStatusClusterJ.ContainerStatusDTO containerStatusDTO = session.
                newInstance(ContainerStatusClusterJ.ContainerStatusDTO.class, pk);
        containerStatusDTO = session.load(containerStatusDTO);
        containerStatusDTOs.add(containerStatusDTO);
      }
    }

    Map<Integer, List<UpdatedContainerInfo>> hopUpdatedContainerInfo
            = updatedContainerDA.findByRMNode(nodeId);
    if (hopUpdatedContainerInfo != null) {
      for (Integer uciId : hopUpdatedContainerInfo.keySet()) {
        for (UpdatedContainerInfo hop : hopUpdatedContainerInfo.get(uciId)) {
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
    Resource hopResource = resourceDA.findEntry(nodeId,
            Resource.TOTAL_CAPABILITY,
            Resource.RMNODE);

    List<ContainerId> hopContainerIdsToClean = containerToCleanDA.
            findByRMNode(nodeId);

    List<FinishedApplications> hopFinishedApplications
            = finishedApplicationsDA.findByRMNode(nodeId);

    RMNode hopRMNode = null;
    Node hopNode = null;
    NodeHBResponse hopNodeHBResponse = null;
    NextHeartbeat hopNextHeartbeat = null;
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

    return new RMNodeComps(hopRMNode, hopNextHeartbeat, hopNode,
            hopNodeHBResponse, hopResource, hopJustLaunchedContainers,
            hopUpdatedContainerInfo, hopContainerIdsToClean,
            hopFinishedApplications,
            ContainerStatusClusterJ.createMap(containerStatusDTOs));
  }

}
