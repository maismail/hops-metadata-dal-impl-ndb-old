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
import se.sics.hop.metadata.hdfs.entity.yarn.HopJustLaunchedContainers;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.JustLaunchedContainersDataAccess;
import se.sics.hop.metadata.yarn.tabledef.JustLaunchedContainersTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class JustLaunchedContainersClusterJ implements JustLaunchedContainersTableDef, JustLaunchedContainersDataAccess<HopJustLaunchedContainers> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface JustLaunchedContainersDTO {

        @PrimaryKey
        @Column(name = RMNODEID)
        String getrmnodeid();

        void setrmnodeid(String rmnodeid);

        @PrimaryKey
        @Column(name = CONTAINERID)
        String getcontainerid();

        void setcontainerid(String containerid);

        @Column(name = CONTAINERSTATUSID)
        String getcontainerstatusid();

        void setcontainerstatusid(String containerstatusid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopJustLaunchedContainers findEntry(String rmnodeid, int commandport, int containerid) throws StorageException {
        Session session = connector.obtainSession();
        Object[] objarr = new Object[2];
        objarr[0] = rmnodeid;
        objarr[1] = containerid;
        JustLaunchedContainersDTO container = null;
        if (session != null) {
            container = session.find(JustLaunchedContainersDTO.class, objarr);
        }
        if (container == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createJustLaunchedContainers(container);
    }

    @Override
    public void prepare(Collection<HopJustLaunchedContainers> modified, Collection<HopJustLaunchedContainers> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                List<JustLaunchedContainersDTO> toRemove = new ArrayList<JustLaunchedContainersDTO>(removed.size());
                for (HopJustLaunchedContainers hopContainerId : removed) {
                    Object[] objarr = new Object[2];
                    objarr[0] = hopContainerId.getRmnodeid();
                    objarr[1] = hopContainerId.getContainerid();
                    toRemove.add(session.newInstance(JustLaunchedContainersDTO.class, objarr));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<JustLaunchedContainersDTO> toModify = new ArrayList<JustLaunchedContainersDTO>(modified.size());
                for (HopJustLaunchedContainers hop : modified) {
                    toModify.add(createPersistable(hop, session));
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public List<HopJustLaunchedContainers> findAll() throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();

            QueryDomainType<JustLaunchedContainersDTO> dobj = qb.createQueryDefinition(JustLaunchedContainersDTO.class);
            Query<JustLaunchedContainersDTO> query = session.createQuery(dobj);

            List<JustLaunchedContainersDTO> results = query.getResultList();
            return createJustLaunchedContainersList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public List<HopJustLaunchedContainers> findByRMNode(String rmnodeId) throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();

            QueryDomainType<JustLaunchedContainersDTO> dobj = qb.createQueryDefinition(JustLaunchedContainersDTO.class);
            Predicate pred = dobj.get("rmnodeid").equal(dobj.param("rmnodeid"));
            dobj.where(pred);
            Query<JustLaunchedContainersDTO> query = session.createQuery(dobj);
            query.setParameter("rmnodeid", rmnodeId);
            List<JustLaunchedContainersDTO> results = query.getResultList();
            return createJustLaunchedContainersList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createJustLaunchedContainerEntry(HopJustLaunchedContainers entry) throws StorageException {
        Session session = connector.obtainSession();
        createPersistable(entry, session);
    }

    private HopJustLaunchedContainers createJustLaunchedContainers(JustLaunchedContainersDTO dto) {
        HopJustLaunchedContainers hop = new HopJustLaunchedContainers(dto.getrmnodeid(), dto.getcontainerid(), dto.getcontainerstatusid());
        return hop;
    }

    /**
     * Persist new map entry.
     *
     * @param entry
     * @param session
     * @return
     */
    private JustLaunchedContainersDTO createPersistable(HopJustLaunchedContainers entry, Session session) {
        JustLaunchedContainersDTO dto = session.newInstance(JustLaunchedContainersDTO.class);
        dto.setcontainerid(entry.getContainerid());
        dto.setcontainerstatusid(entry.getContainerstatusid());
        dto.setrmnodeid(entry.getRmnodeid());
        return dto;
    }

    private List<HopJustLaunchedContainers> createJustLaunchedContainersList(List<JustLaunchedContainersDTO> results) {
        List<HopJustLaunchedContainers> justLaunchedContainers = new ArrayList<HopJustLaunchedContainers>();
        for (JustLaunchedContainersDTO persistable : results) {
            justLaunchedContainers.add(createJustLaunchedContainers(persistable));
        }
        return justLaunchedContainers;
    }
}
