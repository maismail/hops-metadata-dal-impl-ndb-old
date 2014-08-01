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
        @Column(name = HOSTNAME)
        String gethostname();

        void sethostname(String hostname);

        @PrimaryKey
        @Column(name = COMMANDPORT)
        int getcommandport();

        void setcommandport(int commandport);

        @Column(name = RESPONSE)
        byte[] getresponse();

        void setresponse(byte[] responseid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopNodeHBResponse findById(String hostname, int commandport) throws StorageException {
        Session session = connector.obtainSession();
        NodeHBResponseDTO nodeHBresponseDTO = null;
        if (session != null) {
            Object[] pk = new Object[2];
            pk[0] = hostname;
            pk[1] = commandport;
            nodeHBresponseDTO = session.find(NodeHBResponseDTO.class, pk);
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
                    Object[] pk = new Object[2];
                    pk[0] = nodehbresponse.getHostname();
                    pk[1] = nodehbresponse.getCommandport();
                    NodeHBResponseDTO persistable = session.newInstance(NodeHBResponseDTO.class, pk);
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
        return new HopNodeHBResponse(nodeHBresponseDTO.gethostname(), nodeHBresponseDTO.getcommandport(), nodeHBresponseDTO.getresponse());
    }

    private NodeHBResponseDTO createPersistable(HopNodeHBResponse nodehbresponse, Session session) {
        NodeHBResponseDTO nodeHBResponseDT0 = session.newInstance(NodeHBResponseDTO.class);
        nodeHBResponseDT0.sethostname(nodehbresponse.getHostname());
        nodeHBResponseDT0.setcommandport(nodehbresponse.getCommandport());
        nodeHBResponseDT0.setresponse(nodehbresponse.getResponseid());
        return nodeHBResponseDT0;
    }
}
