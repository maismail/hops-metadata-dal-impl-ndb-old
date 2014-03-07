package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.Collection;
import java.util.List;
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

        NodeDTO nodeDTO = null;
        if (session != null) {
            nodeDTO = session.find(NodeDTO.class, id);
        }
        if (nodeDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopNode(nodeDTO);
    }

    @Override
    public HopNode findByNameLocation(String name, String location) throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();
            QueryDomainType<NodeDTO> dobj = qb.createQueryDefinition(NodeDTO.class);
            Predicate pred1 = dobj.get("name").equal(dobj.param("name"));
            Predicate pred2 = dobj.get("location").equal(dobj.param("location"));
            pred1 = pred1.and(pred2);
            dobj.where(pred1);
            Query<NodeDTO> query = session.createQuery(dobj);
            query.setParameter("name", name);
            query.setParameter("location", location);
            List<NodeDTO> results = query.getResultList();
            if (results != null && !results.isEmpty()) {
                return createHopNode(results.get(0));
            } else {
                throw new StorageException("HOP - findByNameLocation :: Node was not found");
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
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

    private NodeDTO createPersistable(HopNode hopNode, Session session) {
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
