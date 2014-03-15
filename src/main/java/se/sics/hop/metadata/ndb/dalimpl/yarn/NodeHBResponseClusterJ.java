package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;
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
        @Column(name = ID)
        int getid();

        void setid(int id);

        @Column(name = RESPONSE_ID)
        int getresponseid();

        void setresponseid(int responseid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopNodeHBResponse findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        NodeHBResponseDTO nodeHBresponseDTO = null;
        if (session != null) {
            nodeHBresponseDTO = session.find(NodeHBResponseDTO.class, id);
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
                for (HopNodeHBResponse nodehbresponse : removed) {

                    NodeHBResponseDTO persistable = session.newInstance(NodeHBResponseDTO.class, nodehbresponse.getId());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopNodeHBResponse nodehbresponse : modified) {
                    NodeHBResponseDTO persistable = createPersistable(nodehbresponse, session);
                    session.savePersistent(persistable);
                }
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
        return new HopNodeHBResponse(nodeHBresponseDTO.getid(), nodeHBresponseDTO.getresponseid());
    }

    private NodeHBResponseDTO createPersistable(HopNodeHBResponse nodehbresponse, Session session) {
        NodeHBResponseDTO nodeHBResponseDT0 = session.newInstance(NodeHBResponseDTO.class);
        nodeHBResponseDT0.setid(nodehbresponse.getId());
        nodeHBResponseDT0.setresponseid(nodehbresponse.getResponseid());
        session.savePersistent(nodeHBResponseDT0);
        return nodeHBResponseDT0;
    }
}
