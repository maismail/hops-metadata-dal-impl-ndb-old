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
import se.sics.hop.metadata.hdfs.entity.yarn.HopRMContextInactiveNodes;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.RMContextInactiveNodesDataAccess;
import se.sics.hop.metadata.yarn.tabledef.RMContextInactiveNodesTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class RMContextInactiveNodesClusterJ implements RMContextInactiveNodesTableDef, RMContextInactiveNodesDataAccess<HopRMContextInactiveNodes> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface RMContextInactiveNodesDTO {

        @PrimaryKey
        @Column(name = RMCONTEXT_ID)
        int getrmcontextid();

        void setrmcontextid(int rmcontextid);

        @Column(name = HOST)
        String gethost();

        void sethost(String host);

        @Column(name = RMNODE_ID)
        int getrmnodeid();

        void setrmnodeid(int rmnodeid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopRMContextInactiveNodes findEntry(int rmcontextId, String host) throws StorageException {
        Session session = connector.obtainSession();
        Object[] objarr = new Object[2];
        objarr[0] = rmcontextId;
        objarr[1] = host;
        RMContextInactiveNodesDTO entry = null;
        if (session != null) {
            entry = session.find(RMContextInactiveNodesDTO.class, objarr);
        }
        if (entry == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createRMContextInactiveNodesEntry(entry);
    }

    @Override
    public List<HopRMContextInactiveNodes> getAllByRMContextId(int rmcontextId) throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();

            QueryDomainType<RMContextInactiveNodesDTO> dobj = qb.createQueryDefinition(RMContextInactiveNodesDTO.class);
            Predicate pred1 = dobj.get("rmcontextid").equal(dobj.param("rmcontextid"));
            dobj.where(pred1);
            Query<RMContextInactiveNodesDTO> query = session.createQuery(dobj);
            query.setParameter("rmcontextid", rmcontextId);

            List<RMContextInactiveNodesDTO> results = query.getResultList();
            return createRMContextInactiveNodesList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void prepare(Collection<HopRMContextInactiveNodes> modified, Collection<HopRMContextInactiveNodes> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopRMContextInactiveNodes entry : removed) {
                    Object[] objarr = new Object[2];
                    objarr[0] = entry.getRmcontextid();
                    objarr[1] = entry.getHost();
                    RMContextInactiveNodesDTO persistable = session.newInstance(RMContextInactiveNodesDTO.class, objarr);
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopRMContextInactiveNodes entry : modified) {
                    RMContextInactiveNodesDTO persistable = createPersistable(entry, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createRMContextInactiveNodesEntry(HopRMContextInactiveNodes entry) throws StorageException {
        Session session = connector.obtainSession();
        createPersistable(entry, session);
    }

    private HopRMContextInactiveNodes createRMContextInactiveNodesEntry(RMContextInactiveNodesDTO entry) {
        return new HopRMContextInactiveNodes(entry.getrmcontextid(), entry.gethost(), entry.getrmnodeid());
    }

    private RMContextInactiveNodesDTO createPersistable(HopRMContextInactiveNodes entry, Session session) {
        Object[] objarr = new Object[2];
        objarr[0] = entry.getRmcontextid();
        objarr[1] = entry.getHost();
        RMContextInactiveNodesDTO persistable = session.newInstance(RMContextInactiveNodesDTO.class, objarr);
        persistable.setrmcontextid(entry.getRmcontextid());
        persistable.sethost(entry.getHost());
        persistable.setrmnodeid(entry.getRmnodeId());
        session.savePersistent(persistable);
        return persistable;
    }

    private List<HopRMContextInactiveNodes> createRMContextInactiveNodesList(List<RMContextInactiveNodesDTO> results) {
        List<HopRMContextInactiveNodes> rmcontextnodes = new ArrayList<HopRMContextInactiveNodes>();
        for (RMContextInactiveNodesDTO persistable : results) {
            rmcontextnodes.add(createRMContextInactiveNodesEntry(persistable));
        }
        return rmcontextnodes;
    }
}
