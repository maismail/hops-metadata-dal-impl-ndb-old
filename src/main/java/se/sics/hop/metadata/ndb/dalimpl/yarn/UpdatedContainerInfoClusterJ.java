package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopUpdatedContainerInfo;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.UpdatedContainerInfoDataAccess;
import se.sics.hop.metadata.yarn.tabledef.UpdatedContainerInfoTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class UpdatedContainerInfoClusterJ implements UpdatedContainerInfoTableDef, UpdatedContainerInfoDataAccess<HopUpdatedContainerInfo> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface UpdatedContainerInfoDTO {

        @PrimaryKey
        @Column(name = RMNODEID)
        String getrmnodeid();

        void setrmnodeid(String rmnodeid);

        @PrimaryKey
        @Column(name = ID)
        int getid();

        void setid(int id);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public void clear(List<HopUpdatedContainerInfo> list) throws StorageException {
        List<UpdatedContainerInfoDTO> toRemove = new ArrayList<UpdatedContainerInfoDTO>();
        Session session = connector.obtainSession();
        for (HopUpdatedContainerInfo hop : list) {
            UpdatedContainerInfoDTO uci = session.newInstance(UpdatedContainerInfoDTO.class);
            uci.setid(hop.getId());
            toRemove.add(uci);
        }
        session.deletePersistentAll(toRemove);
    }

    @Override
    public HopUpdatedContainerInfo findEntry(String rmnodeid, int id) throws StorageException {
        Session session = connector.obtainSession();
        Object[] pk = new Object[2];
        pk[0] = rmnodeid;
        pk[1] = id;
        UpdatedContainerInfoDTO dto = session.find(UpdatedContainerInfoDTO.class, pk);
        if (dto == null) {
            throw new StorageException("Error while retrieving updatedcontainerinfo:" + id + "," + rmnodeid);
        }
        return createHopUpdatedContainerInfo(dto);
    }

    @Override
    public List<HopUpdatedContainerInfo> findByRMNode(String rmnodeid) throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();

            QueryDomainType<UpdatedContainerInfoDTO> dobj = qb.createQueryDefinition(UpdatedContainerInfoDTO.class);
            Predicate pred1 = dobj.get("rmnodeid").equal(dobj.param("rmnodeid"));
            dobj.where(pred1);

            Query<UpdatedContainerInfoDTO> query = session.createQuery(dobj);
            query.setParameter("rmnodeid", rmnodeid);
            List<UpdatedContainerInfoDTO> results = query.getResultList();
            return createUpdatedContainerInfoList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void prepare(Collection<HopUpdatedContainerInfo> modified, Collection<HopUpdatedContainerInfo> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                List<UpdatedContainerInfoDTO> toRemove = new ArrayList<UpdatedContainerInfoDTO>();
                for (HopUpdatedContainerInfo entry : removed) {
                    toRemove.add(createPersistable(entry, session));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<UpdatedContainerInfoDTO> toModify = new ArrayList<UpdatedContainerInfoDTO>();
                for (HopUpdatedContainerInfo entry : modified) {
                    toModify.add(createPersistable(entry, session));
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException("Error while rmnode table:" + e.getMessage());
        }
    }

    private UpdatedContainerInfoDTO createPersistable(HopUpdatedContainerInfo hop, Session session) {
        UpdatedContainerInfoDTO dto = session.newInstance(UpdatedContainerInfoDTO.class);
        dto.setid(hop.getId());
        dto.setrmnodeid(hop.getRmnodeid());
        return dto;
    }

    /**
     * Transforms a DTO to Hop object.
     *
     * @param rmDTO
     * @return HopRMNode
     */
    private HopUpdatedContainerInfo createHopUpdatedContainerInfo(UpdatedContainerInfoDTO dto) {
        return new HopUpdatedContainerInfo(dto.getrmnodeid(), dto.getid());
    }

    private List<HopUpdatedContainerInfo> createUpdatedContainerInfoList(List<UpdatedContainerInfoDTO> list) throws IOException {
        List<HopUpdatedContainerInfo> updatedContainerInfos = new ArrayList<HopUpdatedContainerInfo>();
        for (UpdatedContainerInfoDTO persistable : list) {
            updatedContainerInfos.add(createHopUpdatedContainerInfo(persistable));
        }
        return updatedContainerInfos;
    }
}
