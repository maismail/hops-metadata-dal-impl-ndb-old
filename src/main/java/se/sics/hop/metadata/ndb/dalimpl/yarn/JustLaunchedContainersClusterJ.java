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
import se.sics.hop.metadata.hdfs.entity.yarn.HopJustLaunchedContainers;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsPredicate;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.JustLaunchedContainersDataAccess;
import se.sics.hop.metadata.yarn.tabledef.JustLaunchedContainersTableDef;

public class JustLaunchedContainersClusterJ implements JustLaunchedContainersTableDef, JustLaunchedContainersDataAccess<HopJustLaunchedContainers> {

  private static final Log LOG = LogFactory.getLog(
          JustLaunchedContainersClusterJ.class);

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

  }
  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public void addAll(Collection<HopJustLaunchedContainers> containers) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<JustLaunchedContainersDTO> toModify
            = new ArrayList<JustLaunchedContainersDTO>(containers.size());
    for (HopJustLaunchedContainers hop : containers) {
      toModify.add(createPersistable(hop, session));
    }
    session.savePersistentAll(toModify);
    session.flush();
  }

  @Override
  public void removeAll(Collection<HopJustLaunchedContainers> containers) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<JustLaunchedContainersDTO> toRemove
            = new ArrayList<JustLaunchedContainersDTO>(containers.size());
    for (HopJustLaunchedContainers hopContainerId : containers) {
      Object[] objarr = new Object[2];
      objarr[0] = hopContainerId.getRmnodeid();
      objarr[1] = hopContainerId.getContainerId();
      toRemove.add(session.newInstance(JustLaunchedContainersDTO.class, objarr));
    }
    session.deletePersistentAll(toRemove);
    session.flush();
  }

  @Override
  public List<HopJustLaunchedContainers> findByRMNode(String rmnodeId) throws
          StorageException {
    LOG.debug("HOP :: ClusterJ JustLaunchedContainers.findByRMNode - START:"
            + rmnodeId);
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<JustLaunchedContainersDTO> dobj = qb.
            createQueryDefinition(JustLaunchedContainersDTO.class);
    HopsPredicate pred = dobj.get(RMNODEID).equal(dobj.param(RMNODEID));
    dobj.where(pred);
    HopsQuery<JustLaunchedContainersDTO> query = session.createQuery(dobj);
    query.setParameter(RMNODEID, rmnodeId);
    List<JustLaunchedContainersDTO> results = query.getResultList();
    LOG.debug("HOP :: ClusterJ JustLaunchedContainers.findByRMNode - FINISH:"
            + rmnodeId);
    if (results != null && !results.isEmpty()) {
      return createJustLaunchedContainersList(results);
    }
    return null;
  }

  @Override
  public Map<String, List<HopJustLaunchedContainers>> getAll() throws
          StorageException {
    LOG.debug("HOP :: ClusterJ JustLaunchedContainers.getAll - START");
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<JustLaunchedContainersDTO> dobj = qb.
            createQueryDefinition(
                    JustLaunchedContainersDTO.class);
    HopsQuery<JustLaunchedContainersDTO> query = session.createQuery(dobj);
    List<JustLaunchedContainersDTO> results = query.getResultList();
    LOG.debug("HOP :: ClusterJ JustLaunchedContainers.getAll - FINISH");
    if (results != null && !results.isEmpty()) {
      return createMap(results);
    } else {
      return null;
    }
  }

  private HopJustLaunchedContainers createJustLaunchedContainers(
          JustLaunchedContainersDTO dto) {
    HopJustLaunchedContainers hop = new HopJustLaunchedContainers(dto.
            getrmnodeid(), dto.getcontainerid());
    return hop;
  }

  /**
   * Persist new map entry.
   *
   * @param entry
   * @param session
   * @return
   */
  private JustLaunchedContainersDTO createPersistable(
          HopJustLaunchedContainers entry, HopsSession session) throws
          StorageException {
    JustLaunchedContainersDTO dto = session.newInstance(
            JustLaunchedContainersDTO.class);
    dto.setcontainerid(entry.getContainerId());
    dto.setrmnodeid(entry.getRmnodeid());
    return dto;
  }

  private List<HopJustLaunchedContainers> createJustLaunchedContainersList(
          List<JustLaunchedContainersDTO> results) {
    List<HopJustLaunchedContainers> justLaunchedContainers
            = new ArrayList<HopJustLaunchedContainers>();
    for (JustLaunchedContainersDTO persistable : results) {
      justLaunchedContainers.add(createJustLaunchedContainers(persistable));
    }
    return justLaunchedContainers;
  }

  private Map<String, List<HopJustLaunchedContainers>> createMap(
          List<JustLaunchedContainersDTO> results) {
    Map<String, List<HopJustLaunchedContainers>> map
            = new HashMap<String, List<HopJustLaunchedContainers>>();
    for (JustLaunchedContainersDTO persistable : results) {
      HopJustLaunchedContainers hop = createJustLaunchedContainers(persistable);
      if (map.get(hop.getRmnodeid()) == null) {
        map.put(hop.getRmnodeid(), new ArrayList<HopJustLaunchedContainers>());
      }
      map.get(hop.getRmnodeid()).add(hop);
    }
    return map;
  }
}
