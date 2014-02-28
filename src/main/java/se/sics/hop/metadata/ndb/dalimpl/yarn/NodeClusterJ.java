package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopNode;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.NodeDataAccess;
import se.sics.hop.metadata.yarn.tabledef.NodeTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class NodeClusterJ implements NodeTableDef, NodeDataAccess<HopNode> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface NodeDTO {

        @PrimaryKey
        @Column(name = ID)
        int getId();

        void setId(int id);

        @Column(name = NAME)
        String getName();

        void setName(String host);

        @Column(name = LOCATION)
        String getLocation();

        void setLocation(String location);

        @Column(name = LEVEL)
        int getLevel();

        void setLevel(int level);

        @Column(name = PARENT)
        int getParent();

        void setParent(int parent);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopNode findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        NodeClusterJ.NodeDTO nodeDTO = null;
        if (session != null) {
            nodeDTO = session.find(NodeClusterJ.NodeDTO.class, id);
        }
        if (nodeDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopNode(nodeDTO);
    }

    @Override
    public void prepare(Collection<HopNode> modified, Collection<HopNode> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopNode node : removed) {

                    NodeDTO persistable = session.newInstance(NodeDTO.class, node.getId());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopNode node : modified) {
                    NodeDTO persistable = createPersistable(node, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createNode(HopNode node, int id) throws StorageException {
        Session session = connector.obtainSession();
        createPersistable(node, session);
    }

    private NodeClusterJ.NodeDTO createPersistable(HopNode hopNode, Session session) {
        NodeClusterJ.NodeDTO nodeDTO = session.newInstance(NodeClusterJ.NodeDTO.class);
        //Set values to persist new rmnode
        nodeDTO.setId(hopNode.getId());
        nodeDTO.setName(hopNode.getName());
        nodeDTO.setLocation(hopNode.getLocation());
        nodeDTO.setLevel(hopNode.getLevel());
        nodeDTO.setParent(hopNode.getParent());
        session.savePersistent(nodeDTO);
        return nodeDTO;
    }

    /**
     * Transforms a DTO to Hop object.
     *
     * @param rmDTO
     * @return HopRMNode
     */
    private HopNode createHopNode(NodeDTO nodeDTO) {
        return new HopNode(nodeDTO.getId(), nodeDTO.getName(), nodeDTO.getLocation(), nodeDTO.getLevel(), nodeDTO.getParent());
    }
}
