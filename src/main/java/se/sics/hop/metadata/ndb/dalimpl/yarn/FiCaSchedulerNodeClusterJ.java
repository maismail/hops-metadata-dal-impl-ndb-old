package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopFiCaSchedulerNode;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.FiCaSchedulerNodeDataAccess;
import se.sics.hop.metadata.yarn.tabledef.FiCaSchedulerNodeTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class FiCaSchedulerNodeClusterJ implements FiCaSchedulerNodeTableDef, FiCaSchedulerNodeDataAccess<HopFiCaSchedulerNode> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface FiCaSchedulerNodeDTO {

        @PrimaryKey
        @Column(name = ID)
        int getid();

        void setid(int id);

        @Column(name = AVAILABLE_RESOURCE_ID)
        int getavailableresourceid();

        void setavailableresourceid(int availableresourceid);

        @Column(name = NUMCONTAINERS)
        int getnumcontainers();

        void setnumcontainers(int numcontainers);

        @Column(name = RMNODE_ID)
        int getrmnodeid();

        void setrmnodeid(int rmnodeid);

        @Column(name = TOTAL_CAPABILITY_ID)
        int gettotalcapabilityid();

        void settotalcapabilityid(int totalcapabilityid);

        @Column(name = USER_RESOURCE_ID)
        int getusedresourceid();

        void setusedresourceid(int usedresourceid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopFiCaSchedulerNode findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        FiCaSchedulerNodeDTO ficaschedulernodeDTO = null;
        if (session != null) {
            ficaschedulernodeDTO = session.find(FiCaSchedulerNodeDTO.class, id);
        }
        if (ficaschedulernodeDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopFiCaSchedulerNode(ficaschedulernodeDTO);
    }

    @Override
    public void prepare(Collection<HopFiCaSchedulerNode> modified, Collection<HopFiCaSchedulerNode> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopFiCaSchedulerNode hop : removed) {

                    FiCaSchedulerNodeDTO persistable = session.newInstance(FiCaSchedulerNodeDTO.class, hop.getId());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopFiCaSchedulerNode hop : modified) {
                    FiCaSchedulerNodeDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createFiCaSchedulerNode(HopFiCaSchedulerNode node) throws StorageException {
        Session session = connector.obtainSession();
        createPersistable(node, session);
    }

    private HopFiCaSchedulerNode createHopFiCaSchedulerNode(FiCaSchedulerNodeDTO ficaschedulernodeDTO) {
        HopFiCaSchedulerNode hop = new HopFiCaSchedulerNode(ficaschedulernodeDTO.getid(),
                ficaschedulernodeDTO.getavailableresourceid(),
                ficaschedulernodeDTO.getusedresourceid(),
                ficaschedulernodeDTO.gettotalcapabilityid(),
                ficaschedulernodeDTO.getnumcontainers(),
                ficaschedulernodeDTO.getrmnodeid());
        return null;

    }

    private FiCaSchedulerNodeDTO createPersistable(HopFiCaSchedulerNode hop, Session session) {
        FiCaSchedulerNodeDTO ficaDTO = session.newInstance(FiCaSchedulerNodeDTO.class);
        ficaDTO.setid(hop.getId());
        ficaDTO.setavailableresourceid(hop.getAvailableResourceID());
        ficaDTO.setnumcontainers(hop.getNumOfContainers());
        ficaDTO.setrmnodeid(hop.getRmNodeID());
        ficaDTO.settotalcapabilityid(hop.getTotalCapabilityID());
        ficaDTO.setusedresourceid(hop.getUsedResourceID());
        session.savePersistent(ficaDTO);
        return ficaDTO;
    }
}
