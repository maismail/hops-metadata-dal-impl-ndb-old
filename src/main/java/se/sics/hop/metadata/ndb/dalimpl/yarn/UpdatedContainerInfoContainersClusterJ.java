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
import se.sics.hop.metadata.hdfs.entity.yarn.HopUpdatedContainerInfoContainers;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.UpdatedContainerInfoContainersDataAccess;
import se.sics.hop.metadata.yarn.tabledef.UpdatedContainerInfoContainersTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class UpdatedContainerInfoContainersClusterJ implements UpdatedContainerInfoContainersTableDef, UpdatedContainerInfoContainersDataAccess<HopUpdatedContainerInfoContainers> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface UpdatedContainerInfoContainersDTO {

        @PrimaryKey
        @Column(name = CONTAINERSTATUS_ID)
        String getcontainerstatusid();

        void setcontainerstatusid(String containerstatusid);

        @PrimaryKey
        @Column(name = UPDATEDCONTAINERINFO_ID)
        int getupdatedcontainerinfoid();

        void setupdatedcontainerinfoid(int updatedcontainerinfoid);

        @PrimaryKey
        @Column(name = TYPE)
        int gettype();

        void settype(int type);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopUpdatedContainerInfoContainers findEntry(int containerstatusId, int uciId, int type) throws StorageException {
        Session session = connector.obtainSession();
        UpdatedContainerInfoContainersDTO dto = null;
        Object[] arr = new Object[3];
        arr[0] = containerstatusId;
        arr[1] = uciId;
        arr[2] = type;
        if (session != null) {
            dto = session.find(UpdatedContainerInfoContainersDTO.class, arr);
        }
        if (dto == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopUpdatedContainerInfoContainers(dto);
    }

    @Override
    public List<HopUpdatedContainerInfoContainers> findByUpdatedContainerInfoId(int uciId) throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();

            QueryDomainType<UpdatedContainerInfoContainersDTO> dobj = qb.createQueryDefinition(UpdatedContainerInfoContainersDTO.class);
            Predicate pred = dobj.get("updatedcontainerinfoid").equal(dobj.param("updatedcontainerinfoid"));
            dobj.where(pred);
            Query<UpdatedContainerInfoContainersDTO> query = session.createQuery(dobj);
            query.setParameter("updatedcontainerinfoid", uciId);
            List<UpdatedContainerInfoContainersDTO> results = query.getResultList();
            return createHopUpdatedContainerInfoContainersList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void prepare(Collection<HopUpdatedContainerInfoContainers> modified, Collection<HopUpdatedContainerInfoContainers> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                List<UpdatedContainerInfoContainersDTO> toRemove = new ArrayList<UpdatedContainerInfoContainersDTO>();
                for (HopUpdatedContainerInfoContainers entry : removed) {
                    toRemove.add(createPersistable(entry, session));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<UpdatedContainerInfoContainersDTO> toModify = new ArrayList<UpdatedContainerInfoContainersDTO>();
                for (HopUpdatedContainerInfoContainers entry : modified) {
                    toModify.add(createPersistable(entry, session));
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException("Error while rmnode table:" + e.getMessage());
        }
    }

    /**
     * Transforms a DTO to Hop object.
     *
     * @param rmDTO
     * @return HopRMNode
     */
    private HopUpdatedContainerInfoContainers createHopUpdatedContainerInfoContainers(UpdatedContainerInfoContainersDTO dto) {
        return new HopUpdatedContainerInfoContainers(dto.getcontainerstatusid(), dto.getupdatedcontainerinfoid(), dto.gettype());
    }

    private UpdatedContainerInfoContainersDTO createPersistable(HopUpdatedContainerInfoContainers hop, Session session) {
        UpdatedContainerInfoContainersDTO dto = session.newInstance(UpdatedContainerInfoContainersDTO.class);
        //Set values to persist new rmnode
        dto.setcontainerstatusid(hop.getContainerstatusId());
        dto.setupdatedcontainerinfoid(hop.getUpdatedcontainerinfoId());
        dto.settype(hop.getType());
        return dto;
    }

    private List<HopUpdatedContainerInfoContainers> createHopUpdatedContainerInfoContainersList(List<UpdatedContainerInfoContainersDTO> results) {
        List<HopUpdatedContainerInfoContainers> hopList = new ArrayList<HopUpdatedContainerInfoContainers>();
        for (UpdatedContainerInfoContainersDTO persistable : results) {
            hopList.add(createHopUpdatedContainerInfoContainers(persistable));
        }
        return hopList;
    }
}
