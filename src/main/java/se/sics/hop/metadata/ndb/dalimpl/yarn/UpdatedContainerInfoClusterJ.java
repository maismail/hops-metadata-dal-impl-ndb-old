package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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


public class UpdatedContainerInfoClusterJ implements
        UpdatedContainerInfoTableDef,
        UpdatedContainerInfoDataAccess<HopUpdatedContainerInfo> {

  private static final Log LOG = LogFactory.getLog(NodeClusterJ.class);

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

    @PrimaryKey
    @Column(name = UPDATEDCONTAINERINFOID)
    int getupdatedcontainerinfoid();

    void setupdatedcontainerinfoid(int updatedcontainerinfoid);

  }
  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public Map<Integer, List<HopUpdatedContainerInfo>> findByRMNode(
          String rmnodeid) throws StorageException {
    LOG.debug("HOP :: ClusterJ UpdatedContainerInfo.findByRMNode - START:"
            + rmnodeid);
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<UpdatedContainerInfoDTO> dobj = qb.
            createQueryDefinition(UpdatedContainerInfoDTO.class);
    HopsPredicate pred1 = dobj.get(RMNODEID).equal(dobj.param(RMNODEID));
    dobj.where(pred1);

    HopsQuery<UpdatedContainerInfoDTO> query = session.createQuery(dobj);
    query.setParameter(RMNODEID, rmnodeid);
    List<UpdatedContainerInfoDTO> results = query.getResultList();
    LOG.debug("HOP :: ClusterJ UpdatedContainerInfo.findByRMNode - FINISH:"
            + rmnodeid);
    if (results != null && !results.isEmpty()) {
      return createUpdatedContainerInfoMap(results);
    }
    return null;
  }

  @Override
  public Map<String, Map<Integer, List<HopUpdatedContainerInfo>>> getAll()
          throws
          StorageException {
    LOG.debug("HOP :: ClusterJ UpdatedContainerInfo.findByRMNode - START");
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<UpdatedContainerInfoDTO> dobj = qb.
            createQueryDefinition(
                    UpdatedContainerInfoDTO.class);
    HopsQuery<UpdatedContainerInfoDTO> query = session.createQuery(dobj);

    List<UpdatedContainerInfoDTO> results = query.getResultList();
    LOG.debug("HOP :: ClusterJ UpdatedContainerInfo.findByRMNode - FINISH");
    return createMap(results);
  }

  @Override
  public void addAll(Collection<HopUpdatedContainerInfo> containers) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<UpdatedContainerInfoDTO> toModify
            = new ArrayList<UpdatedContainerInfoDTO>();
    for (HopUpdatedContainerInfo entry : containers) {
      toModify.add(createPersistable(entry, session));
    }
    session.savePersistentAll(toModify);
    session.flush();
  }

  @Override
  public void removeAll(Collection<HopUpdatedContainerInfo> containers) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<UpdatedContainerInfoDTO> toRemove
            = new ArrayList<UpdatedContainerInfoDTO>();
    for (HopUpdatedContainerInfo entry : containers) {
      toRemove.add(createPersistable(entry, session));
    }
    session.deletePersistentAll(toRemove);
    session.flush();
  }

  private UpdatedContainerInfoDTO createPersistable(HopUpdatedContainerInfo hop,
          HopsSession session) throws StorageException {
    UpdatedContainerInfoDTO dto = session.newInstance(
            UpdatedContainerInfoDTO.class);
    dto.setrmnodeid(hop.getRmnodeid());
    dto.setcontainerid(hop.getContainerId());
    dto.setupdatedcontainerinfoid(hop.getUpdatedContainerInfoId());
    return dto;
  }

  /**
   * Transforms a DTO to Hop object.
   *
   * @param rmDTO
   * @return HopRMNode
   */
  private HopUpdatedContainerInfo createHopUpdatedContainerInfo(
          UpdatedContainerInfoDTO dto) {
    return new HopUpdatedContainerInfo(dto.getrmnodeid(), dto.getcontainerid(),
            dto.getupdatedcontainerinfoid());
  }

  private Map<Integer, List<HopUpdatedContainerInfo>> createUpdatedContainerInfoMap(
          List<UpdatedContainerInfoDTO> list) {
    Map<Integer, List<HopUpdatedContainerInfo>> updatedContainerInfos
            = new HashMap<Integer, List<HopUpdatedContainerInfo>>();
    for (UpdatedContainerInfoDTO persistable : list) {
      if (!updatedContainerInfos.containsKey(persistable.
              getupdatedcontainerinfoid())) {
        updatedContainerInfos.put(persistable.getupdatedcontainerinfoid(),
                new ArrayList<HopUpdatedContainerInfo>());
      }
      updatedContainerInfos.get(persistable.getupdatedcontainerinfoid()).add(
              createHopUpdatedContainerInfo(persistable));
    }
    return updatedContainerInfos;
  }

  private Map<String, Map<Integer, List<HopUpdatedContainerInfo>>> createMap(
          List<UpdatedContainerInfoDTO> results) {
    Map<String, Map<Integer, List<HopUpdatedContainerInfo>>> map
            = new HashMap<String, Map<Integer, List<HopUpdatedContainerInfo>>>();
    for (UpdatedContainerInfoDTO persistable : results) {
      HopUpdatedContainerInfo hop = createHopUpdatedContainerInfo(persistable);
      if (map.get(hop.getRmnodeid()) == null) {
        map.put(hop.getRmnodeid(),
                new HashMap<Integer, List<HopUpdatedContainerInfo>>());
      }
      if (map.get(hop.getRmnodeid()).get(hop.getUpdatedContainerInfoId())
              == null) {
        map.get(hop.getRmnodeid()).put(hop.getUpdatedContainerInfoId(),
                new ArrayList<HopUpdatedContainerInfo>());
      }
      map.get(hop.getRmnodeid()).get(hop.getUpdatedContainerInfoId()).add(hop);
    }
    return map;
  }
}
