package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Lob;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopNodeId;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.NodeIdDataAccess;
import se.sics.hop.metadata.yarn.tabledef.NodeIdTableDef;
import static se.sics.hop.metadata.yarn.tabledef.NodeIdTableDef.TABLE_NAME;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class NodeIdClusterJ implements NodeIdTableDef, NodeIdDataAccess<HopNodeId> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface NodeIdDTO {

        @Column(name = ID)
        int getId();

        void setId(int id);

        @PrimaryKey
        @Column(name = HOST)
        String getHost();

        void setHost(String host);

        @PrimaryKey
        @Column(name = PORT)
        int getPort();

        void setPort(int port);

        @Lob
        @Column(name = "nodeser")
        byte[] getNodeser();

        void setNodeser(byte[] nodeser);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopNodeId findByHostPort(String host, int port) throws StorageException {
        Session session = connector.obtainSession();
        Object[] objarr = new Object[2];
        objarr[0] = host;
        objarr[1] = port;
        NodeIdDTO nodeidDTO = null;
        if (session != null) {
            nodeidDTO = session.find(NodeIdDTO.class, objarr);
            session.flush();
            session.close();
        }
        if (nodeidDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopNodeId(nodeidDTO);
    }

    @Override
    public void prepare(Collection<HopNodeId> modified, Collection<HopNodeId> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopNodeId nodeid : removed) {
                    Object[] objarr = new Object[2];
                    objarr[0] = nodeid.getHost();
                    objarr[1] = nodeid.getPort();
                    NodeIdDTO persistable = session.newInstance(NodeIdDTO.class, objarr);
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopNodeId nodeid : modified) {
                    NodeIdDTO persistable = createPersistable(nodeid, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createNodeId(HopNodeId nodeId) throws StorageException {
        Session session = connector.obtainSession();
        createPersistable(nodeId, session);
    }

    private NodeIdDTO createPersistable(HopNodeId hopRMNode, Session session) {
        NodeIdDTO nodeDTO = session.newInstance(NodeIdDTO.class);

        //Set values to persist new rmnode
        nodeDTO.setId(hopRMNode.getId());
        nodeDTO.setHost(hopRMNode.getHost());
        nodeDTO.setPort(hopRMNode.getPort());
        nodeDTO.setNodeser(hopRMNode.getNodeser());
        session.savePersistent(nodeDTO);
        return nodeDTO;
    }

    /**
     * Transforms a DTO to Hop object.
     *
     * @param rmDTO
     * @return HopRMNode
     */
    private HopNodeId createHopNodeId(NodeIdDTO nodeidDTO) {
        return new HopNodeId(nodeidDTO.getId(), nodeidDTO.getHost(), nodeidDTO.getPort(), nodeidDTO.getNodeser());
    }
}
