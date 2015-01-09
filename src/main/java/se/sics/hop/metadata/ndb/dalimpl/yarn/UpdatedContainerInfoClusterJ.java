package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopUpdatedContainerInfo;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsPredicate;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
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
        @Column(name = CONTAINERID)
        String getcontainerid();
        
        void setcontainerid(String containerid);

    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public void clear(List<HopUpdatedContainerInfo> list) throws StorageException {
        List<UpdatedContainerInfoDTO> toRemove = new ArrayList<UpdatedContainerInfoDTO>();
        HopsSession session = connector.obtainSession();
        for (HopUpdatedContainerInfo hop : list) {
            UpdatedContainerInfoDTO uci = session.newInstance(UpdatedContainerInfoDTO.class);
            uci.setrmnodeid(hop.getRmnodeid());
            toRemove.add(uci);
        }
        session.deletePersistentAll(toRemove);
    }

    @Override
    public HopUpdatedContainerInfo findEntry(String rmnodeid, int id) throws StorageException {
        HopsSession session = connector.obtainSession();
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
            HopsSession session = connector.obtainSession();
            HopsQueryBuilder qb = session.getQueryBuilder();

            HopsQueryDomainType<UpdatedContainerInfoDTO> dobj = qb.createQueryDefinition(UpdatedContainerInfoDTO.class);
            HopsPredicate pred1 = dobj.get("rmnodeid").equal(dobj.param("rmnodeid"));
            dobj.where(pred1);

            HopsQuery<UpdatedContainerInfoDTO> query = session.createQuery(dobj);
            query.setParameter("rmnodeid", rmnodeid);
            List<UpdatedContainerInfoDTO> results = query.getResultList();
            return createUpdatedContainerInfoList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
  @Override
  public Map<String, List<HopUpdatedContainerInfo>> getAll() throws
          StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<UpdatedContainerInfoDTO> dobj = qb.createQueryDefinition(
            UpdatedContainerInfoDTO.class);
    HopsQuery<UpdatedContainerInfoDTO> query = session.createQuery(dobj);

    List<UpdatedContainerInfoDTO> results = query.getResultList();
    return createMap(results);
  }
    
    @Override
    public void prepare(Collection<HopUpdatedContainerInfo> modified, Collection<HopUpdatedContainerInfo> removed) throws StorageException {
        HopsSession session = connector.obtainSession();
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

    private UpdatedContainerInfoDTO createPersistable(HopUpdatedContainerInfo hop, HopsSession session) throws StorageException {
        UpdatedContainerInfoDTO dto = session.newInstance(UpdatedContainerInfoDTO.class);
        dto.setrmnodeid(hop.getRmnodeid());
        dto.setcontainerid(hop.getContainerId());
        return dto;
    }

    /**
     * Transforms a DTO to Hop object.
     *
     * @param rmDTO
     * @return HopRMNode
     */
    private HopUpdatedContainerInfo createHopUpdatedContainerInfo(UpdatedContainerInfoDTO dto) {
        return new HopUpdatedContainerInfo(dto.getrmnodeid(), dto.getcontainerid());
    }

    private List<HopUpdatedContainerInfo> createUpdatedContainerInfoList(List<UpdatedContainerInfoDTO> list) throws IOException {
        List<HopUpdatedContainerInfo> updatedContainerInfos = new ArrayList<HopUpdatedContainerInfo>();
        for (UpdatedContainerInfoDTO persistable : list) {
            updatedContainerInfos.add(createHopUpdatedContainerInfo(persistable));
        }
        return updatedContainerInfos;
    }
    
  private Map<String, List<HopUpdatedContainerInfo>> createMap(
          List<UpdatedContainerInfoDTO> results) {
    Map<String, List<HopUpdatedContainerInfo>> map
            = new HashMap<String, List<HopUpdatedContainerInfo>>();
    for (UpdatedContainerInfoDTO persistable : results) {
      HopUpdatedContainerInfo hop = createHopUpdatedContainerInfo(persistable);
      if (map.get(hop.getRmnodeid()) == null) {
        map.put(hop.getRmnodeid(), new ArrayList<HopUpdatedContainerInfo>());
      }
      map.get(hop.getRmnodeid()).add(hop);
    }
    return map;
  }
}
