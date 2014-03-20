package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopFifoSchedulerNodes;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.FifoSchedulerNodesDataAccess;
import se.sics.hop.metadata.yarn.tabledef.FifoSchedulerNodesTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class FifoSchedulerNodesClusterJ implements FifoSchedulerNodesTableDef, FifoSchedulerNodesDataAccess<HopFifoSchedulerNodes> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface FifoSchedulerNodesDTO {

        @PrimaryKey
        @Column(name = FIFOSCHEDULER_ID)
        int getfifoschedulerid();

        void setfifoschedulerid(int fifoschedulerid);

        @PrimaryKey
        @Column(name = NODEID_ID)
        int getnodeidid();

        void setnodeidid(int nodeidid);

        @Column(name = FICASCHEDULERNODE_ID)
        int getficaschedulernodeid();

        void setficaschedulernodeid(int ficaschedulernodeid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopFifoSchedulerNodes findEntry(int nodeidId, int fiCaSchedulerNodeId) throws StorageException {
        Session session = connector.obtainSession();
        Object[] objarr = new Object[2];
        objarr[0] = nodeidId;
        objarr[1] = fiCaSchedulerNodeId;
        FifoSchedulerNodesDTO entry = null;
        if (session != null) {
            entry = session.find(FifoSchedulerNodesDTO.class, objarr);
        }
        if (entry == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createFifoSchedulerNodes(entry);
    }

    @Override
    public void prepare(Collection<HopFifoSchedulerNodes> modified, Collection<HopFifoSchedulerNodes> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopFifoSchedulerNodes hop : removed) {
                    Object[] objarr = new Object[2];
                    objarr[0] = hop.getFifoSchedulerID();
                    objarr[1] = hop.getNodeidID();
                    FifoSchedulerNodesDTO persistable = session.newInstance(FifoSchedulerNodesDTO.class, objarr);
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopFifoSchedulerNodes id : modified) {
                    FifoSchedulerNodesDTO persistable = createPersistable(id, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createFifoSchedulerNodesEntry(HopFifoSchedulerNodes entry) throws StorageException {
        Session session = connector.obtainSession();
        createPersistable(entry, session);
    }

    private HopFifoSchedulerNodes createFifoSchedulerNodes(FifoSchedulerNodesDTO entry) {
        HopFifoSchedulerNodes hop = new HopFifoSchedulerNodes(entry.getfifoschedulerid(), entry.getnodeidid(), entry.getficaschedulernodeid());
        return hop;
    }

    private FifoSchedulerNodesDTO createPersistable(HopFifoSchedulerNodes id, Session session) {
        FifoSchedulerNodesDTO fifoDTO = session.newInstance(FifoSchedulerNodesDTO.class);
        fifoDTO.setfifoschedulerid(id.getFifoSchedulerID());
        fifoDTO.setnodeidid(id.getNodeidID());
        fifoDTO.setficaschedulernodeid(id.getFicaSchedulerNodeID());
        session.savePersistent(fifoDTO);
        return fifoDTO;
    }
}