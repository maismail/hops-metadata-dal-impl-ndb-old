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
        @Column(name = ID)
        int getid();

        void setid(int id);

        @Column(name = RMNODE_ID)
        int getrmnodeid();

        void setrmnodeid(int rmnodeid);
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
    public HopUpdatedContainerInfo findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        UpdatedContainerInfoDTO uciDTO = null;
        if (session != null) {
            uciDTO = session.find(UpdatedContainerInfoDTO.class, id);
        }
        if (uciDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopUpdatedContainerInfo(uciDTO);
    }

    @Override
    public List<HopUpdatedContainerInfo> findByRMNodeId(int rmnodeid) throws StorageException {
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

    @Override
    public void createUpdatedContainerInfo(HopUpdatedContainerInfo updatedContainerInfo) throws StorageException {
        Session session = connector.obtainSession();
        session.savePersistent(createPersistable(updatedContainerInfo, session));
    }

    private UpdatedContainerInfoDTO createPersistable(HopUpdatedContainerInfo hopUCI, Session session) {
        UpdatedContainerInfoDTO uciDTO = session.newInstance(UpdatedContainerInfoDTO.class);
        //Set values to persist new rmnode
        uciDTO.setid(hopUCI.getId());
        if (hopUCI.getRmnodeid() != Integer.MIN_VALUE) {
            uciDTO.setrmnodeid(hopUCI.getRmnodeid());
        }
        return uciDTO;
    }

    /**
     * Transforms a DTO to Hop object.
     *
     * @param rmDTO
     * @return HopRMNode
     */
    private HopUpdatedContainerInfo createHopUpdatedContainerInfo(UpdatedContainerInfoDTO uciDTO) {
        return new HopUpdatedContainerInfo(uciDTO.getid(), uciDTO.getrmnodeid());
    }

    private List<HopUpdatedContainerInfo> createUpdatedContainerInfoList(List<UpdatedContainerInfoDTO> list) throws IOException {
        List<HopUpdatedContainerInfo> updatedContainerInfos = new ArrayList<HopUpdatedContainerInfo>();
        for (UpdatedContainerInfoDTO persistable : list) {
            updatedContainerInfos.add(createHopUpdatedContainerInfo(persistable));
        }
        return updatedContainerInfos;
    }
}
