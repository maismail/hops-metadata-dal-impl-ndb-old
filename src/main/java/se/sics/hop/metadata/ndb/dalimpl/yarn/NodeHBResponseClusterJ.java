package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
}
