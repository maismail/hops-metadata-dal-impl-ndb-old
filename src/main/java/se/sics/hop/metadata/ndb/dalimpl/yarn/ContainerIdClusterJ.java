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
import se.sics.hop.metadata.hdfs.entity.yarn.HopContainerId;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.ContainerIdDataAccess;
import se.sics.hop.metadata.yarn.tabledef.ContainerIdTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class ContainerIdClusterJ implements ContainerIdTableDef, ContainerIdDataAccess<HopContainerId> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface ContainerIdDTO {

        @PrimaryKey
        @Column(name = ID)
        int getid();

        void setid(int id);

        @Column(name = CONTAINERID)
        int getcontid();

        void setcontid(int contid);

        @Column(name = APPLICATIONATTEMPT_ID)
        int getapplicationattemptid();

        void setapplicationattemptid(int applicationattemptid);

        @Column(name = TO_CLEAN)
        int gettoclean();

        void settoclean(int toclean);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopContainerId findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        ContainerIdDTO containerIdDTO = null;


        if (session != null) {
            containerIdDTO = session.find(ContainerIdDTO.class, id);
        }
        if (containerIdDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopContainerId(containerIdDTO);
    }

    @Override
    public List<HopContainerId> findContainerIdsToClean() throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();
            QueryDomainType<ContainerIdDTO> dobj = qb.createQueryDefinition(ContainerIdDTO.class);
            Predicate pred1 = dobj.get("toclean").equal(dobj.param("toclean"));

            dobj.where(pred1);
            Query<ContainerIdDTO> query = session.createQuery(dobj);

            query.setParameter(
                    "toclean", 1);
            List<ContainerIdDTO> results = query.getResultList();

            return createContainerIdToCleanList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public HopContainerId findByIdStatus(int id, int toclean) throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();
            QueryDomainType<ContainerIdDTO> dobj = qb.createQueryDefinition(ContainerIdDTO.class);
            Predicate pred1 = dobj.get("id").equal(dobj.param("id"));
            Predicate pred2 = dobj.get("toclean").equal(dobj.param("toclean"));
            pred1 = pred1.and(pred2);
            dobj.where(pred1);
            Query<ContainerIdDTO> query = session.createQuery(dobj);
            query.setParameter("id", id);
            query.setParameter("toclean", 1);
            List<ContainerIdDTO> results = query.getResultList();
            if (results != null && !results.isEmpty()) {
                return createHopContainerId(results.get(0));
            } else {
                throw new StorageException("HOP - ContainerIdToClean was not found");
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void prepare(Collection<HopContainerId> modified, Collection<HopContainerId> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopContainerId hopContainerId : removed) {
                    ContainerIdDTO persistable = session.newInstance(ContainerIdDTO.class, hopContainerId.getNdbId());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopContainerId hopContainerId : modified) {
                    ContainerIdDTO persistable = createPersistable(hopContainerId, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createContainerId(HopContainerId containerstatus) throws StorageException {
        Session session = connector.obtainSession();
        createPersistable(containerstatus, session);
    }

    private ContainerIdDTO createPersistable(HopContainerId hopContainerId, Session session) {
        ContainerIdDTO containerIdDTO = session.newInstance(ContainerIdDTO.class);
        //Set values to persist new ContainerStatus
        containerIdDTO.setid(hopContainerId.getNdbId());
        containerIdDTO.setcontid(hopContainerId.getContid());
        containerIdDTO.setapplicationattemptid(hopContainerId.getApplicationAttemptId());
        containerIdDTO.settoclean(hopContainerId.getToClean());
        session.savePersistent(containerIdDTO);
        return containerIdDTO;
    }

    private HopContainerId createHopContainerId(ContainerIdDTO containerIdDTO) {
        HopContainerId hop = new HopContainerId(containerIdDTO.getid(), containerIdDTO.getcontid(), containerIdDTO.getapplicationattemptid(), containerIdDTO.gettoclean());
        return hop;
    }

    private List<HopContainerId> createContainerIdToCleanList(List<ContainerIdDTO> results) {
        List<HopContainerId> containerIdsToClean = new ArrayList<HopContainerId>();
        for (ContainerIdDTO persistable : results) {
            containerIdsToClean.add(createHopContainerId(persistable));
        }
        return containerIdsToClean;
    }
}
