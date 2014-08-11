package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopNodeId;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.NodeIdDataAccess;
import se.sics.hop.metadata.yarn.tabledef.NodeIdTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class NodeIdClusterJ implements NodeIdTableDef, NodeIdDataAccess<HopNodeId> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface NodeIdDTO {

        @PrimaryKey
        @Column(name = ID)
        String getId();

        String setId(String id);

        @Column(name = HOST)
        String getHost();

        void setHost(String host);

        @Column(name = PORT)
        int getPort();

        void setPort(int port);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopNodeId findById(int id) throws StorageException {
        NodeIdDTO nodeidDTO = null;
        try {
            Session session = connector.obtainSession();
            if (session != null) {
                nodeidDTO = session.find(NodeIdDTO.class, id);
            }
            if (nodeidDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row:" + id);
            }
            return createHopNodeId(nodeidDTO);
        } catch (Exception e) {
            if (e.getMessage().contains("Tuple did not exist")) {
                throw new StorageException("HOP :: Error while retrieving row:" + id);
            }
        }
        throw new StorageException("HOP :: Error while retrieving row:" + id);
    }

    @Override
    public HopNodeId findByHostPort(String host, int port) throws StorageException {

        List<NodeIdDTO> results;
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();

            QueryDomainType<NodeIdDTO> dobj = qb.createQueryDefinition(NodeIdDTO.class);
            Predicate pred1 = dobj.get("host").equal(dobj.param("host"));
            Predicate pred2 = dobj.get("port").equal(dobj.param("port"));
            pred1.and(pred2);
            dobj.where(pred1);
            Query<NodeIdDTO> query = session.createQuery(dobj);
            query.setParameter("host", host);
            query.setParameter("port", port);
            results = query.getResultList();

            if (results != null && !results.isEmpty()) {
                return createHopNodeId(results.get(0));
            }
        } catch (Exception e) {
            if (e.getMessage().contains("Tuple did not exist")) {
                throw new StorageException("HOP :: Error while retrieving row:" + host + ":" + port);
            }
        }

        throw new StorageException("HOP :: Error while retrieving row:" + host + ":" + port);
    }

    @Override
    public void prepare(Collection<HopNodeId> modified, Collection<HopNodeId> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                List<NodeIdDTO> toRemove = new ArrayList<NodeIdDTO>();
                for (HopNodeId req : removed) {
                    toRemove.add(session.newInstance(NodeIdDTO.class, req.getId()));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<NodeIdDTO> toModify = new ArrayList<NodeIdDTO>();
                for (HopNodeId req : modified) {
                    toModify.add(createPersistable(req, session));
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException("Error while modifying NodeIds, error:" + e.getMessage());
        }
    }

    @Override
    public void deleteAll(int startId, int endId) throws StorageException {
        Session session = connector.obtainSession();
        //session.deletePersistentAll(NodeIdDTO.class);
        for (int i = startId; i < endId; i++) {
            QueryBuilder qb = session.getQueryBuilder();

            QueryDomainType<NodeIdDTO> dobj = qb.createQueryDefinition(NodeIdDTO.class);
            Predicate pred1 = dobj.get("id").equal(dobj.param("id"));
            dobj.where(pred1);
            Query<NodeIdDTO> query = session.createQuery(dobj);
            query.setParameter("id", i);

            List<NodeIdDTO> results = query.getResultList();
            try {
                NodeIdDTO del = session.newInstance(NodeIdDTO.class);
                del.setHost(results.get(0).getHost());
                del.setPort(results.get(0).getPort());
                Object[] objarr = new Object[2];
                objarr[0] = results.get(0).getHost();
                objarr[1] = results.get(0).getPort();
                NodeIdDTO nodeidDTO = session.find(NodeIdDTO.class, objarr);

                session.deletePersistent(nodeidDTO);
            } catch (Exception e) {
            }
        }
    }

    @Override
    public void createNodeId(HopNodeId nodeId) throws StorageException {
        Session session = connector.obtainSession();
        session.savePersistent(createPersistable(nodeId, session));
    }

    private NodeIdDTO createPersistable(HopNodeId hopNodeId, Session session) {
        NodeIdDTO nodeDTO = session.newInstance(NodeIdDTO.class);
        //Set values to persist new rmnode
        nodeDTO.setId(hopNodeId.getId());
        nodeDTO.setHost(hopNodeId.getHost());
        nodeDTO.setPort(hopNodeId.getPort());
        return nodeDTO;
    }

    /**
     * Transforms a DTO to Hop object.
     *
     * @param rmDTO
     * @return HopRMNode
     */
    private HopNodeId createHopNodeId(NodeIdDTO nodeidDTO) {
        return new HopNodeId(nodeidDTO.getId(), nodeidDTO.getHost(), nodeidDTO.getPort());
    }
}
