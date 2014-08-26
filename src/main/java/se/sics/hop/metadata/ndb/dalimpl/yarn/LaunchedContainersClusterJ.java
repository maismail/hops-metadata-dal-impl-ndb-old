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
        @Column(name = FICASCHEDULERNODE_ID)
        String getficaschedulernode_id();

        void setficaschedulernode_id(String ficaschedulernode_id);

        @PrimaryKey
        @Column(name = CONTAINERID_ID)
        String getcontaineridid();

        void setcontaineridid(String containeridid);

        @Column(name = RMCONTAINER_ID)
        String getrmcontainerid();

        void setrmcontainerid(String rmcontainerid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopLaunchedContainers findEntry(String ficaschedulernodeId, String containeridId) throws StorageException {
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
    public List<HopLaunchedContainers> findByFiCaSchedulerNode(String ficaschedulernode_id) throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();

            QueryDomainType<LaunchedContainersDTO> dobj = qb.createQueryDefinition(LaunchedContainersDTO.class);
            Predicate pred1 = dobj.get("ficaschedulernode_id").equal(dobj.param("ficaschedulernode_id"));
            dobj.where(pred1);
            Query<LaunchedContainersDTO> query = session.createQuery(dobj);
            query.setParameter("ficaschedulernode_id", ficaschedulernode_id);

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
                List<LaunchedContainersDTO> toRemove = new ArrayList<LaunchedContainersDTO>(removed.size());
                for (HopLaunchedContainers hopContainerId : removed) {
                    Object[] objarr = new Object[2];
                    objarr[0] = hopContainerId.getFicaSchedulerNodeID();
                    objarr[1] = hopContainerId.getContainerIdID();
                    toRemove.add(session.newInstance(LaunchedContainersDTO.class, objarr));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<LaunchedContainersDTO> toModify = new ArrayList<LaunchedContainersDTO>(modified.size());
                for (HopLaunchedContainers id : modified) {
                    toModify.add(createPersistable(id, session));
                }
                session.savePersistentAll(toModify);
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
                dto.getficaschedulernode_id(),
                dto.getcontaineridid(),
                dto.getrmcontainerid());
        return hop;
    }

    private LaunchedContainersDTO createPersistable(HopLaunchedContainers entry, Session session) {
        Object[] objarr = new Object[2];
        objarr[0] = entry.getFicaSchedulerNodeID();
        objarr[1] = entry.getContainerIdID();
        LaunchedContainersDTO persistable = session.newInstance(LaunchedContainersDTO.class, objarr);
        persistable.setficaschedulernode_id(entry.getFicaSchedulerNodeID());
        persistable.setcontaineridid(entry.getContainerIdID());
        persistable.setrmcontainerid(entry.getRmContainerID());
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
