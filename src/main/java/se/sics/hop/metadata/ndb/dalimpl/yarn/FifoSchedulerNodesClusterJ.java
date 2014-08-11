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
        @Column(name = NODEID_ID)
        String getnodeidid();

        void setnodeidid(String nodeidid);

        @Column(name = FICASCHEDULERNODE_ID)
        String getficaschedulernodeid();

        void setficaschedulernodeid(String ficaschedulernodeid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopFifoSchedulerNodes findById(String nodeid) throws StorageException {
        Session session = connector.obtainSession();
        FifoSchedulerNodesDTO entry = null;
        if (session != null) {
            entry = session.find(FifoSchedulerNodesDTO.class, nodeid);
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
                List<FifoSchedulerNodesDTO> toRemove = new ArrayList<FifoSchedulerNodesDTO>();
                for (HopFifoSchedulerNodes hop : removed) {
                    toRemove.add(session.newInstance(FifoSchedulerNodesDTO.class, hop.getNodeidID()));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<FifoSchedulerNodesDTO> toModify = new ArrayList<FifoSchedulerNodesDTO>();
                for (HopFifoSchedulerNodes id : modified) {
                    toModify.add(createPersistable(id, session));
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createFifoSchedulerNodesEntry(HopFifoSchedulerNodes entry) throws StorageException {
        Session session = connector.obtainSession();
        session.savePersistent(createPersistable(entry, session));
    }

    @Override
    public List<HopFifoSchedulerNodes> getAll() throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();

            QueryDomainType<FifoSchedulerNodesClusterJ.FifoSchedulerNodesDTO> dobj = qb.createQueryDefinition(FifoSchedulerNodesClusterJ.FifoSchedulerNodesDTO.class);
            Query<FifoSchedulerNodesClusterJ.FifoSchedulerNodesDTO> query = session.createQuery(dobj);

            List<FifoSchedulerNodesClusterJ.FifoSchedulerNodesDTO> results = query.getResultList();
            return createFifoSchedulerNodesList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    private HopFifoSchedulerNodes createFifoSchedulerNodes(FifoSchedulerNodesDTO entry) {
        HopFifoSchedulerNodes hop = new HopFifoSchedulerNodes(entry.getnodeidid(), entry.getficaschedulernodeid());
        return hop;
    }

    private FifoSchedulerNodesDTO createPersistable(HopFifoSchedulerNodes id, Session session) {
        FifoSchedulerNodesDTO fifoDTO = session.newInstance(FifoSchedulerNodesDTO.class);
        fifoDTO.setnodeidid(id.getNodeidID());
        fifoDTO.setficaschedulernodeid(id.getFicaSchedulerNodeID());
        return fifoDTO;
    }

    private List<HopFifoSchedulerNodes> createFifoSchedulerNodesList(List<FifoSchedulerNodesClusterJ.FifoSchedulerNodesDTO> results) {
        List<HopFifoSchedulerNodes> fifoSchedulerNodes = new ArrayList<HopFifoSchedulerNodes>();
        for (FifoSchedulerNodesClusterJ.FifoSchedulerNodesDTO persistable : results) {
            fifoSchedulerNodes.add(createFifoSchedulerNodes(persistable));
        }
        return fifoSchedulerNodes;
    }
}
