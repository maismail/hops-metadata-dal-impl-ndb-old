package io.hops.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.hops.exception.StorageException;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.tabledef.NodeHBResponseTableDef;
import io.hops.util.CompressionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.zip.DataFormatException;

import io.hops.metadata.yarn.entity.NodeHBResponse;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.yarn.dal.NodeHBResponseDataAccess;

public class NodeHBResponseClusterJ implements NodeHBResponseTableDef, NodeHBResponseDataAccess<NodeHBResponse> {

  private static final Log LOG = LogFactory.getLog(NodeHBResponseClusterJ.class);

  @PersistenceCapable(table = TABLE_NAME)
  public interface NodeHBResponseDTO extends RMNodeComponentDTO {

    @PrimaryKey
    @Column(name = RMNODEID)
    String getrmnodeid();

    void setrmnodeid(String rmnodeid);

    @Column(name = RESPONSE)
    byte[] getresponse();

    void setresponse(byte[] responseid);
  }
  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public NodeHBResponse findById(String rmnodeId) throws StorageException {
    LOG.debug("HOP :: ClusterJ NodeHBResponse.findById - START:" + rmnodeId);
    HopsSession session = connector.obtainSession();
    NodeHBResponseDTO nodeHBresponseDTO;
    if (session != null) {
      nodeHBresponseDTO = session.find(NodeHBResponseDTO.class, rmnodeId);
      LOG.debug("HOP :: ClusterJ NodeHBResponse.findById - FINISH:" + rmnodeId);
      if (nodeHBresponseDTO != null) {
        return createHopNodeHBResponse(nodeHBresponseDTO);
      }
    }
    LOG.debug("HOP :: ClusterJ NodeHBResponse.findById.session_null - FINISH:"
            + rmnodeId);
    session.flush();
    return null;
  }

  @Override
  public Map<String, NodeHBResponse> getAll() throws StorageException {
    LOG.debug("HOP :: ClusterJ NodeHBResponse.getAll - START");
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<NodeHBResponseDTO> dobj
            = qb.createQueryDefinition(
                    NodeHBResponseDTO.class);
    HopsQuery<NodeHBResponseDTO> query = session.
            createQuery(dobj);
    List<NodeHBResponseDTO> results = query.
            getResultList();
    LOG.debug("HOP :: ClusterJ NodeHBResponse.getAll - FINISH");
    return createMap(results);
  }

  @Override
  public void add(NodeHBResponse toAdd) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    session.savePersistent(createPersistable(toAdd, session));
  }

  public static NodeHBResponse createHopNodeHBResponse(
          NodeHBResponseDTO nodeHBresponseDTO)
          throws StorageException {
    try {
      return new NodeHBResponse(nodeHBresponseDTO.getrmnodeid(), CompressionUtils
          .
              decompress(nodeHBresponseDTO.getresponse()));
    } catch (IOException e) {
      throw new StorageException(e);
    } catch (DataFormatException e) {
      throw new StorageException(e);

    }
  }

  private NodeHBResponseDTO createPersistable(NodeHBResponse nodehbresponse,
          HopsSession session) throws StorageException {
    NodeHBResponseDTO nodeHBResponseDT0 = session.newInstance(
            NodeHBResponseDTO.class);
    nodeHBResponseDT0.setrmnodeid(nodehbresponse.getRMNodeId());
    try {
      nodeHBResponseDT0.setresponse(CompressionUtils.compress(nodehbresponse.
              getResponse()));
    } catch (IOException e) {
      throw new StorageException(e);
    }
    return nodeHBResponseDT0;
  }

  private Map<String, NodeHBResponse> createMap(
          List<NodeHBResponseDTO> results) throws StorageException {
    Map<String, NodeHBResponse> map
            = new HashMap<String, NodeHBResponse>();
    for (NodeHBResponseDTO dto : results) {
      NodeHBResponse hop = createHopNodeHBResponse(dto);
      map.put(hop.getRMNodeId(), hop);
    }
    return map;
  }
}
