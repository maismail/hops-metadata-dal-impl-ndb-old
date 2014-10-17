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
import java.util.List;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopRMContextActiveNodes;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.RMContextActiveNodesDataAccess;

import se.sics.hop.metadata.yarn.tabledef.RMContextActiveNodesTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class RMContextActiveNodesClusterJ implements RMContextActiveNodesTableDef, RMContextActiveNodesDataAccess<HopRMContextActiveNodes> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface RMContextNodesDTO {

        @PrimaryKey
        @Column(name = RMNODEID)
        String getnodeidid();

        void setnodeidid(String nodeidid);

    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopRMContextActiveNodes findEntry(int rmcontextId, int nodeidId) throws StorageException {
        Session session = connector.obtainSession();
        RMContextNodesDTO entry = null;
        if (session != null) {
            entry = session.find(RMContextNodesDTO.class, nodeidId);
        }
        if (entry == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }
        return createRMContextNodesEntry(entry);
    }

    @Override
    public List<HopRMContextActiveNodes> findAll() throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();

            QueryDomainType<RMContextNodesDTO> dobj = qb.createQueryDefinition(RMContextNodesDTO.class);
            Query<RMContextNodesDTO> query = session.createQuery(dobj);

            List<RMContextNodesDTO> results = query.getResultList();
            return createRMContextNodesList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void prepare(Collection<HopRMContextActiveNodes> modified, Collection<HopRMContextActiveNodes> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                List<RMContextNodesDTO> toRemove = new ArrayList<RMContextNodesDTO>();
                for (HopRMContextActiveNodes entry : removed) {
                    toRemove.add(session.newInstance(RMContextNodesDTO.class, entry.getNodeidId()));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<RMContextNodesDTO> toModify = new ArrayList<RMContextNodesDTO>();
                for (HopRMContextActiveNodes req : modified) {
                    toModify.add(createPersistable(req, session));
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException("Error while modifying invokerequests, error:" + e.getMessage());
        }
    }

    @Override
    public void createRMContextNodesEntry(HopRMContextActiveNodes entry) throws StorageException {
        Session session = connector.obtainSession();
        session.savePersistent(createPersistable(entry, session));
    }

    private HopRMContextActiveNodes createRMContextNodesEntry(RMContextNodesDTO entry) {
        return new HopRMContextActiveNodes(entry.getnodeidid());
    }

    private RMContextNodesDTO createPersistable(HopRMContextActiveNodes entry, Session session) {
        RMContextNodesDTO persistable = session.newInstance(RMContextNodesDTO.class, entry.getNodeidId());
        persistable.setnodeidid(entry.getNodeidId());
        //session.savePersistent(persistable);
        return persistable;
    }

    private List<HopRMContextActiveNodes> createRMContextNodesList(List<RMContextNodesDTO> results) {
        List<HopRMContextActiveNodes> rmcontextNodes = new ArrayList<HopRMContextActiveNodes>();
        for (RMContextNodesDTO persistable : results) {
            rmcontextNodes.add(createRMContextNodesEntry(persistable));
        }
        return rmcontextNodes;
    }
}
