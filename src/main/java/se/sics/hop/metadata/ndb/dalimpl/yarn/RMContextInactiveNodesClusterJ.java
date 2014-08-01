package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
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
        @Column(name = HOST)
        String gethost();

        void sethost(String host);

        @Column(name = RMNODEID)
        String getrmnodeid();

        void setrmnodeid(String rmnodeid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopRMContextInactiveNodes findById(String host) throws StorageException {
        Session session = connector.obtainSession();

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
    public void prepare(Collection<HopRMContextInactiveNodes> modified, Collection<HopRMContextInactiveNodes> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                List<RMContextInactiveNodesDTO> toRemove = new ArrayList<RMContextInactiveNodesDTO>();
                for (HopRMContextInactiveNodes entry : removed) {
                    toRemove.add(session.newInstance(RMContextInactiveNodesDTO.class, entry.getHost()));
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
        Session session = connector.obtainSession();
        createPersistable(entry, session);
    }

    private HopRMContextInactiveNodes createRMContextInactiveNodesEntry(RMContextInactiveNodesDTO entry) {
        return new HopRMContextInactiveNodes(entry.gethost(), entry.getrmnodeid());
    }

    private RMContextInactiveNodesDTO createPersistable(HopRMContextInactiveNodes entry, Session session) {
        RMContextInactiveNodesDTO persistable = session.newInstance(RMContextInactiveNodesDTO.class);
        persistable.sethost(entry.getHost());
        persistable.setrmnodeid(entry.getRmnodeid());
        return persistable;
    }
}
