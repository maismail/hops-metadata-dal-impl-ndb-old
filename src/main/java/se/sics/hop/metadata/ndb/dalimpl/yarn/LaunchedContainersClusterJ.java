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
import se.sics.hop.metadata.hdfs.entity.yarn.HopLaunchedContainers;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.LaunchedContainersDataAccess;
import se.sics.hop.metadata.yarn.tabledef.LaunchedContainersTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class LaunchedContainersClusterJ implements LaunchedContainersTableDef, LaunchedContainersDataAccess<HopLaunchedContainers> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface LaunchedContainersDTO {

        @PrimaryKey
        @Column(name = RMCONTAINER_ID)
        int getrmcontainerid();

        void setrmcontainerid(int rmcontainerid);

        @PrimaryKey
        @Column(name = CONTAINERID_ID)
        int getcontaineridid();

        void setcontaineridid(int containeridid);

        @Column(name = FICASCHEDULERNODE_ID)
        int getficaschedulernodeid();

        void setficaschedulernodeid(int ficaschedulernodeid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopLaunchedContainers findEntry(int ficaschedulernodeId, int containeridId) throws StorageException {
        Session session = connector.obtainSession();
        Object[] objarr = new Object[2];
        objarr[0] = ficaschedulernodeId;
        objarr[1] = containeridId;
        LaunchedContainersDTO dto = null;
        if (session != null) {
            dto = session.find(LaunchedContainersDTO.class, objarr);
        }
        if (dto == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createLaunchedContainersEntry(dto);
    }

    @Override
    public List<HopLaunchedContainers> findByFiCaSchedulerNode(int ficaschedulernodeId) throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();

            QueryDomainType<LaunchedContainersDTO> dobj = qb.createQueryDefinition(LaunchedContainersDTO.class);
            Predicate pred1 = dobj.get("ficaschedulernode_id").equal(dobj.param("ficaschedulernode_id"));
            dobj.where(pred1);
            Query<LaunchedContainersDTO> query = session.createQuery(dobj);
            query.setParameter("ficaschedulernode_id", ficaschedulernodeId);

            List<LaunchedContainersDTO> results = query.getResultList();
            return createLaunchedContainersList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void prepare(Collection<HopLaunchedContainers> modified, Collection<HopLaunchedContainers> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopLaunchedContainers hopContainerId : removed) {
                    Object[] objarr = new Object[2];
                    objarr[0] = hopContainerId.getFicaSchedulerNodeID();
                    objarr[1] = hopContainerId.getContainerIdID();
                    LaunchedContainersDTO persistable = session.newInstance(LaunchedContainersDTO.class, objarr);
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopLaunchedContainers id : modified) {
                    LaunchedContainersDTO persistable = createPersistable(id, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createLaunchedContainersEntry(HopLaunchedContainers ficaschedulernode, HopLaunchedContainers containerId) throws StorageException {
        Session session = connector.obtainSession();
        createPersistable(ficaschedulernode, session);
    }

    private HopLaunchedContainers createLaunchedContainersEntry(LaunchedContainersDTO dto) {
        HopLaunchedContainers hop = new HopLaunchedContainers(
                dto.getficaschedulernodeid(),
                dto.getcontaineridid(),
                dto.getrmcontainerid());
        return hop;
    }

    private LaunchedContainersDTO createPersistable(HopLaunchedContainers entry, Session session) {
        Object[] objarr = new Object[2];
        objarr[0] = entry.getFicaSchedulerNodeID();
        objarr[1] = entry.getContainerIdID();
        LaunchedContainersDTO persistable = session.newInstance(LaunchedContainersDTO.class, objarr);
        persistable.setficaschedulernodeid(entry.getFicaSchedulerNodeID());
        persistable.setcontaineridid(entry.getContainerIdID());
        persistable.setrmcontainerid(entry.getRmContainerID());
        session.savePersistent(persistable);
        return persistable;
    }

    private List<HopLaunchedContainers> createLaunchedContainersList(List<LaunchedContainersDTO> results) {
        List<HopLaunchedContainers> launchedContainers = new ArrayList<HopLaunchedContainers>();
        for (LaunchedContainersDTO persistable : results) {
            launchedContainers.add(createLaunchedContainersEntry(persistable));
        }
        return launchedContainers;
    }
}
