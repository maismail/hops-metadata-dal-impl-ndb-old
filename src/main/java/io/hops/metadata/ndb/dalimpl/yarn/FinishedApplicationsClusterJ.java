package io.hops.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.hops.metadata.hdfs.entity.yarn.HopFinishedApplications;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsSession;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.yarn.dal.FinishedApplicationsDataAccess;
import io.hops.metadata.yarn.tabledef.FinishedApplicationsTableDef;

public class FinishedApplicationsClusterJ implements FinishedApplicationsTableDef, FinishedApplicationsDataAccess<HopFinishedApplications> {

  private static final Log LOG = LogFactory.getLog(
          FinishedApplicationsClusterJ.class);

  @PersistenceCapable(table = TABLE_NAME)
  public interface FinishedApplicationsDTO {

    @PrimaryKey
    @Column(name = RMNODEID)
    String getrmnodeid();

    void setrmnodeid(String rmnodeid);

    @PrimaryKey
    @Column(name = APPLICATIONID)
    String getapplicationid();

    void setapplicationid(String applicationid);
  }
  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public List<HopFinishedApplications> findByRMNode(String rmnodeid) throws
          StorageException {
    LOG.debug("HOP :: ClusterJ FinishedApplications.findByRMNode - START:"
            + rmnodeid);
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<FinishedApplicationsDTO> dobj = qb.
            createQueryDefinition(FinishedApplicationsDTO.class);
    HopsPredicate pred1 = dobj.get(RMNODEID).equal(dobj.param(RMNODEID));
    dobj.where(pred1);

    HopsQuery<FinishedApplicationsDTO> query = session.createQuery(dobj);
    query.setParameter(RMNODEID, rmnodeid);
    List<FinishedApplicationsDTO> results = query.getResultList();
    LOG.debug("HOP :: ClusterJ FinishedApplications.findByRMNode - FINISH:"
            + rmnodeid);
    if (results != null && !results.isEmpty()) {
      return createUpdatedContainerInfoList(results);
    }
    return null;

  }

  @Override
  public Map<String, List<HopFinishedApplications>> getAll() throws
          StorageException {
    LOG.debug("HOP :: ClusterJ FinishedApplications.getAll - START");
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<FinishedApplicationsDTO> dobj
            = qb.createQueryDefinition(
                    FinishedApplicationsDTO.class);
    HopsQuery<FinishedApplicationsDTO> query = session.
            createQuery(dobj);
    List<FinishedApplicationsDTO> results = query.
            getResultList();
    LOG.debug("HOP :: ClusterJ FinishedApplications.getAll - FINISH");
    return createMap(results);
  }

  @Override
  public void addAll(Collection<HopFinishedApplications> applications) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<FinishedApplicationsDTO> toModify
            = new ArrayList<FinishedApplicationsDTO>();
    for (HopFinishedApplications entry : applications) {
      toModify.add(createPersistable(entry, session));
    }
    session.savePersistentAll(toModify);
    session.flush();
  }

  @Override
  public void removeAll(Collection<HopFinishedApplications> applications) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<FinishedApplicationsDTO> toRemove
            = new ArrayList<FinishedApplicationsDTO>();
    for (HopFinishedApplications entry : applications) {
      toRemove.add(createPersistable(entry, session));
    }
    session.deletePersistentAll(toRemove);
    session.flush();
  }

  private HopFinishedApplications createHopFinishedApplications(
          FinishedApplicationsDTO dto) {
    return new HopFinishedApplications(dto.getrmnodeid(), dto.getapplicationid());
  }

  private FinishedApplicationsDTO createPersistable(HopFinishedApplications hop,
          HopsSession session) throws StorageException {
    FinishedApplicationsDTO dto = session.newInstance(
            FinishedApplicationsDTO.class);
    dto.setrmnodeid(hop.getRMNodeID());
    dto.setapplicationid(hop.getApplicationId());
    return dto;
  }

  private List<HopFinishedApplications> createUpdatedContainerInfoList(
          List<FinishedApplicationsDTO> list) {
    List<HopFinishedApplications> finishedApps
            = new ArrayList<HopFinishedApplications>();
    for (FinishedApplicationsDTO persistable : list) {
      finishedApps.add(createHopFinishedApplications(persistable));
    }
    return finishedApps;
  }

  private Map<String, List<HopFinishedApplications>> createMap(
          List<FinishedApplicationsDTO> results) {
    Map<String, List<HopFinishedApplications>> map
            = new HashMap<String, List<HopFinishedApplications>>();
    for (FinishedApplicationsDTO dto : results) {
      HopFinishedApplications hop
              = createHopFinishedApplications(dto);
      if (map.get(hop.getRMNodeID()) == null) {
        map.put(hop.getRMNodeID(),
                new ArrayList<HopFinishedApplications>());
      }
      map.get(hop.getRMNodeID()).add(hop);
    }
    return map;
  }
}
