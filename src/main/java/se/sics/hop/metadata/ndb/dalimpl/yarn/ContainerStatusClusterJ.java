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
import se.sics.hop.metadata.hdfs.entity.yarn.HopContainerStatus;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.ContainerStatusDataAccess;
import se.sics.hop.metadata.yarn.tabledef.ContainerStatusTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class ContainerStatusClusterJ implements ContainerStatusTableDef, ContainerStatusDataAccess<HopContainerStatus> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface ContainerStatusDTO {

        @PrimaryKey
        @Column(name = ID)
        int getid();

        void setid(int id);

        @Column(name = STATE)
        String getstate();

        void setstate(String state);

        @Column(name = CONTAINER_ID)
        int getcontainerid();

        void setcontainerid(int containerid);

        @Column(name = DIAGNOSTICS)
        String getdiagnostics();

        void setdiagnostics(String diagnostics);

        @Column(name = EXIT_STATUS)
        int getexitstatus();

        void setexitstatus(int exitstatus);

        @Column(name = TYPE)
        String gettype();

        void settype(String type);

        @Column(name = UPDATEDCONTAINERINFO_ID)
        int getupdatedcontainerinfoid();

        void setupdatedcontainerinfoid(int updatedcontainerinfoid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopContainerStatus findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        ContainerStatusDTO uciDTO = null;
        if (session != null) {
            uciDTO = session.find(ContainerStatusDTO.class, id);
        }
        if (uciDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopContainerStatus(uciDTO);
    }

    @Override
    public List<HopContainerStatus> findByUpdatedContainerInfoId(int uciId) throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();

            QueryDomainType<ContainerStatusDTO> dobj = qb.createQueryDefinition(ContainerStatusDTO.class);
            Predicate pred1 = dobj.get("updatedcontainerinfoid").equal(dobj.param("updatedcontainerinfoid"));
            dobj.where(pred1);
            Query<ContainerStatusDTO> query = session.createQuery(dobj);
            query.setParameter("updatedcontainerinfoid", uciId);

            List<ContainerStatusDTO> results = query.getResultList();
            return createHopContainerStatusList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public List<HopContainerStatus> findByUpdatedContainerInfoIdAndState(int uciId, String state) throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();

            QueryDomainType<ContainerStatusDTO> dobj = qb.createQueryDefinition(ContainerStatusDTO.class);
            Predicate pred1 = dobj.get("updatedcontainerinfoid").equal(dobj.param("updatedcontainerinfoid"));
            Predicate pred2 = dobj.get("state").equal(dobj.param("state"));
            pred1 = pred1.and(pred2);
            dobj.where(pred1);
            Query<ContainerStatusDTO> query = session.createQuery(dobj);
            query.setParameter("updatedcontainerinfoid", uciId);
            query.setParameter("state", state);
            List<ContainerStatusDTO> results = query.getResultList();
            return createHopContainerStatusList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void prepare(Collection<HopContainerStatus> modified, Collection<HopContainerStatus> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopContainerStatus hopUCI : removed) {

                    ContainerStatusDTO persistable = session.newInstance(ContainerStatusDTO.class, hopUCI.getId());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopContainerStatus hopUCI : modified) {
                    ContainerStatusDTO persistable = createPersistable(hopUCI, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createContainerStatus(HopContainerStatus containerstatus) throws StorageException {
        Session session = connector.obtainSession();
        createPersistable(containerstatus, session);
    }

    private ContainerStatusDTO createPersistable(HopContainerStatus hopCS, Session session) {
        ContainerStatusDTO csDTO = session.newInstance(ContainerStatusDTO.class);
        //Set values to persist new ContainerStatus
        csDTO.setid(hopCS.getId());
        csDTO.setstate(hopCS.getState());
        csDTO.setcontainerid(hopCS.getContainerid());
        csDTO.setdiagnostics(hopCS.getDiagnostics());
        csDTO.setexitstatus(hopCS.getExitstatus());
        csDTO.settype(hopCS.getType());
        if (hopCS.getUpdatedcontainerinfoid() != -1) {
            csDTO.setupdatedcontainerinfoid(hopCS.getUpdatedcontainerinfoid());
        }
        session.savePersistent(csDTO);
        return csDTO;
    }

    private HopContainerStatus createHopContainerStatus(ContainerStatusDTO csDTO) {
        HopContainerStatus hop = new HopContainerStatus(csDTO.getid(), csDTO.getstate(), csDTO.getcontainerid(), csDTO.getdiagnostics(), csDTO.getexitstatus(), csDTO.gettype(), csDTO.getupdatedcontainerinfoid());
        return hop;
    }

    private List<HopContainerStatus> createHopContainerStatusList(List<ContainerStatusDTO> listDTO) {
        List<HopContainerStatus> hopList = new ArrayList<HopContainerStatus>();
        for (ContainerStatusDTO persistable : listDTO) {
            hopList.add(createHopContainerStatus(persistable));
        }
        return hopList;
    }
}
