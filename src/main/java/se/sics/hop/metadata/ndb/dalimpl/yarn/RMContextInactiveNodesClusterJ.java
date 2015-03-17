package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopRMContextInactiveNodes;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.RMContextInactiveNodesDataAccess;
import se.sics.hop.metadata.yarn.tabledef.RMContextInactiveNodesTableDef;

public class RMContextInactiveNodesClusterJ implements RMContextInactiveNodesTableDef, RMContextInactiveNodesDataAccess<HopRMContextInactiveNodes> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface RMContextInactiveNodesDTO {

        @PrimaryKey
        @Column(name = RMNODEID)
        String getrmnodeid();

        void setrmnodeid(String rmnodeid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopRMContextInactiveNodes findById(String host) throws StorageException {
        HopsSession session = connector.obtainSession();

        RMContextInactiveNodesDTO entry = null;
        if (session != null) {
            entry = session.find(RMContextInactiveNodesDTO.class, host);
        }
        if (entry == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createRMContextInactiveNodesEntry(entry);
    }

    @Override
    public List<HopRMContextInactiveNodes> findAll() throws StorageException {
        try {
            HopsSession session = connector.obtainSession();
            HopsQueryBuilder qb = session.getQueryBuilder();

            HopsQueryDomainType<RMContextInactiveNodesDTO> dobj = qb.createQueryDefinition(RMContextInactiveNodesDTO.class);
            HopsQuery<RMContextInactiveNodesDTO> query = session.createQuery(dobj);

            List<RMContextInactiveNodesDTO> results = query.getResultList();
            return createRMContextInactiveNodesList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void prepare(Collection<HopRMContextInactiveNodes> modified, Collection<HopRMContextInactiveNodes> removed) throws StorageException {
        HopsSession session = connector.obtainSession();
        try {
            if (removed != null) {
                List<RMContextInactiveNodesDTO> toRemove = new ArrayList<RMContextInactiveNodesDTO>();
                for (HopRMContextInactiveNodes entry : removed) {
                    toRemove.add(session.newInstance(RMContextInactiveNodesDTO.class));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<RMContextInactiveNodesDTO> toModify = new ArrayList<RMContextInactiveNodesDTO>();
                for (HopRMContextInactiveNodes entry : modified) {
                    toModify.add(createPersistable(entry, session));
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createRMContextInactiveNodesEntry(HopRMContextInactiveNodes entry) throws StorageException {
        HopsSession session = connector.obtainSession();
        session.savePersistent(createPersistable(entry, session));
    }

    private HopRMContextInactiveNodes createRMContextInactiveNodesEntry(RMContextInactiveNodesDTO entry) {
        return new HopRMContextInactiveNodes( entry.getrmnodeid());
    }

    private RMContextInactiveNodesDTO createPersistable(HopRMContextInactiveNodes entry, HopsSession session) throws StorageException {
        RMContextInactiveNodesDTO persistable = session.newInstance(RMContextInactiveNodesDTO.class);
        persistable.setrmnodeid(entry.getRmnodeid());
        return persistable;
    }

    private List<HopRMContextInactiveNodes> createRMContextInactiveNodesList(List<RMContextInactiveNodesDTO> results) {
        List<HopRMContextInactiveNodes> rmcontextInactiveNodes = new ArrayList<HopRMContextInactiveNodes>();
        for (RMContextInactiveNodesDTO persistable : results) {
            rmcontextInactiveNodes.add(createRMContextInactiveNodesEntry(persistable));
        }
        return rmcontextInactiveNodes;
    }
}
