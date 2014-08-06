package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
        @Column(name = RMNODEID)
        String getrmnodeid();

        void setrmnodeid(String rmnodeid);

        @Column(name = NODENAME)
        String getnodename();

        void setnodename(String nodename);

        @Column(name = NUMCONTAINERS)
        int getnumcontainers();

        void setnumcontainers(int numcontainers);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopFiCaSchedulerNode findById(String id) throws StorageException {
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
                List<FiCaSchedulerNodeDTO> toRemove = new ArrayList<FiCaSchedulerNodeDTO>();
                for (HopFiCaSchedulerNode hop : removed) {
                    FiCaSchedulerNodeDTO persistable = session.newInstance(FiCaSchedulerNodeDTO.class, hop.getRmnodeId());
                    toRemove.add(persistable);
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<FiCaSchedulerNodeDTO> toModify = new ArrayList<FiCaSchedulerNodeDTO>();
                for (HopFiCaSchedulerNode hop : modified) {
                    FiCaSchedulerNodeDTO persistable = createPersistable(hop, session);
                    toModify.add(persistable);
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createFiCaSchedulerNode(HopFiCaSchedulerNode node) throws StorageException {
        Session session = connector.obtainSession();
        session.savePersistent(createPersistable(node, session));
    }

    private HopFiCaSchedulerNode createHopFiCaSchedulerNode(FiCaSchedulerNodeDTO ficaschedulernodeDTO) {
        HopFiCaSchedulerNode hop = new HopFiCaSchedulerNode(ficaschedulernodeDTO.getrmnodeid(),
                ficaschedulernodeDTO.getnodename(),
                ficaschedulernodeDTO.getnumcontainers());
        return hop;
    }

    private FiCaSchedulerNodeDTO createPersistable(HopFiCaSchedulerNode hop, Session session) {
        FiCaSchedulerNodeDTO ficaDTO = session.newInstance(FiCaSchedulerNodeDTO.class);
        ficaDTO.setrmnodeid(hop.getRmnodeId());
        ficaDTO.setnodename(hop.getNodeName());
        ficaDTO.setnumcontainers(hop.getNumOfContainers());
        return ficaDTO;
    }
}
