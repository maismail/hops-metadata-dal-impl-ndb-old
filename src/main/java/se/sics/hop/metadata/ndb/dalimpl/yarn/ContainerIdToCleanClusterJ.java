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
import se.sics.hop.metadata.hdfs.entity.yarn.HopContainerIdToClean;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.ContainerIdToCleanDataAccess;
import se.sics.hop.metadata.yarn.tabledef.ContainerIdToCleanTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class ContainerIdToCleanClusterJ implements ContainerIdToCleanTableDef, ContainerIdToCleanDataAccess<HopContainerIdToClean> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface ContainerIdToCleanDTO {

        @PrimaryKey
        @Column(name = RMNODEID)
        String getrmnodeid();

        void setrmnodeid(String rmnodeid);

        @PrimaryKey
        @Column(name = CONTAINERID)
        String getcontainerid();

        void setcontainerid(String containerid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopContainerIdToClean findEntry(String rmnodeid, String containerid) throws StorageException {
        Session session = connector.obtainSession();
        ContainerIdToCleanDTO dto = null;
        Object[] pk = new Object[2];
        pk[0] = rmnodeid;
        pk[1] = containerid;
        if (session != null) {
            dto = session.find(ContainerIdToCleanDTO.class, pk);
        }
        if (dto == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopContainerIdToClean(dto);
    }

    @Override
    public List<HopContainerIdToClean> findByRMNode(String rmnodeId) throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();

            QueryDomainType<ContainerIdToCleanDTO> dobj = qb.createQueryDefinition(ContainerIdToCleanDTO.class);
            Predicate pred = dobj.get("rmnodeid").equal(dobj.param("rmnodeid"));
            dobj.where(pred);
            Query<ContainerIdToCleanDTO> query = session.createQuery(dobj);
            query.setParameter("rmnodeid", rmnodeId);
            List<ContainerIdToCleanDTO> results = query.getResultList();
            return createContainersToCleanList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void prepare(Collection<HopContainerIdToClean> modified, Collection<HopContainerIdToClean> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                List<ContainerIdToCleanDTO> toRemove = new ArrayList<ContainerIdToCleanDTO>();
                for (HopContainerIdToClean hop : removed) {
                    toRemove.add(createPersistable(hop, session));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<ContainerIdToCleanDTO> toModify = new ArrayList<ContainerIdToCleanDTO>();
                for (HopContainerIdToClean hop : modified) {
                    toModify.add(createPersistable(hop, session));
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    private ContainerIdToCleanDTO createPersistable(HopContainerIdToClean hop, Session session) {
        ContainerIdToCleanDTO dto = session.newInstance(ContainerIdToCleanDTO.class);
        //Set values to persist new ContainerStatus
        dto.setrmnodeid(hop.getRmnodeid());
        dto.setcontainerid(hop.getContainerId());
        return dto;
    }

    private HopContainerIdToClean createHopContainerIdToClean(ContainerIdToCleanDTO dto) {
        HopContainerIdToClean hop = new HopContainerIdToClean(dto.getrmnodeid(), dto.getcontainerid());
        return hop;
    }

    private List<HopContainerIdToClean> createContainersToCleanList(List<ContainerIdToCleanDTO> results) {
        List<HopContainerIdToClean> containersToClean = new ArrayList<HopContainerIdToClean>();
        for (ContainerIdToCleanDTO persistable : results) {
            containersToClean.add(createHopContainerIdToClean(persistable));
        }
        return containersToClean;
    }
}