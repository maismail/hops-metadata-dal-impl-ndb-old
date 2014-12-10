package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopNodeHBResponse;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.NodeHBResponseDataAccess;
import se.sics.hop.metadata.yarn.tabledef.NodeHBResponseTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class NodeHBResponseClusterJ implements NodeHBResponseTableDef, NodeHBResponseDataAccess<HopNodeHBResponse> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface NodeHBResponseDTO {

        @PrimaryKey
        @Column(name = RMNODEID)
        String getrmnodeid();

        void setrmnodeid(String rmnodeid);

        @Column(name = RESPONSE)
        byte[] getresponse();

        void setresponse(byte[] responseid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopNodeHBResponse findById(String rmnodeId) throws StorageException {
        Session session = connector.obtainSession();
        NodeHBResponseDTO nodeHBresponseDTO = null;
        if (session != null) {
            nodeHBresponseDTO = session.find(NodeHBResponseDTO.class, rmnodeId);
        }
        if (nodeHBresponseDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopNodeHBResponse(nodeHBresponseDTO);
    }

   @Override
  public Map<String, HopNodeHBResponse> getAll() throws StorageException {
    Session session = connector.obtainSession();
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<NodeHBResponseDTO> dobj
            = qb.createQueryDefinition(
                    NodeHBResponseDTO.class);
    Query<NodeHBResponseDTO> query = session.
            createQuery(dobj);
    List<NodeHBResponseDTO> results = query.
            getResultList();
    return createMap(results);
  }
    
    @Override
    public void prepare(Collection<HopNodeHBResponse> modified, Collection<HopNodeHBResponse> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                List<NodeHBResponseDTO> toRemove = new ArrayList<NodeHBResponseDTO>();
                for (HopNodeHBResponse nodehbresponse : removed) {
                    NodeHBResponseDTO persistable = session.newInstance(NodeHBResponseDTO.class, nodehbresponse.getRMNodeId());
                    toRemove.add(persistable);
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<NodeHBResponseDTO> toModify = new ArrayList<NodeHBResponseDTO>();
                for (HopNodeHBResponse nodehbresponse : modified) {
                    toModify.add(createPersistable(nodehbresponse, session));
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createNodeHBResponse(HopNodeHBResponse nodehbresponse) throws StorageException {
        Session session = connector.obtainSession();
        createPersistable(nodehbresponse, session);
    }

    private HopNodeHBResponse createHopNodeHBResponse(NodeHBResponseDTO nodeHBresponseDTO) {
        return new HopNodeHBResponse(nodeHBresponseDTO.getrmnodeid(), nodeHBresponseDTO.getresponse());
    }

    private NodeHBResponseDTO createPersistable(HopNodeHBResponse nodehbresponse, Session session) {
        NodeHBResponseDTO nodeHBResponseDT0 = session.newInstance(NodeHBResponseDTO.class);
        nodeHBResponseDT0.setrmnodeid(nodehbresponse.getRMNodeId());
        nodeHBResponseDT0.setresponse(nodehbresponse.getResponseid());
        return nodeHBResponseDT0;
    }
    
  private Map<String, HopNodeHBResponse> createMap(
          List<NodeHBResponseDTO> results) {
    Map<String, HopNodeHBResponse> map
            = new HashMap<String, HopNodeHBResponse>();
    for (NodeHBResponseDTO dto : results) {
      HopNodeHBResponse hop = createHopNodeHBResponse(dto);
      map.put(hop.getRMNodeId(), hop);
    }
    return map;
  }
}
