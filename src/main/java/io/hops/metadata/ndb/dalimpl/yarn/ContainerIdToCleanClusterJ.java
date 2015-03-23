package io.hops.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.hops.exception.StorageException;
import io.hops.metadata.yarn.entity.HopContainerId;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.tabledef.ContainerIdToCleanTableDef;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.yarn.dal.ContainerIdToCleanDataAccess;

public class ContainerIdToCleanClusterJ implements ContainerIdToCleanTableDef, ContainerIdToCleanDataAccess<HopContainerId> {

  private static final Log LOG = LogFactory.getLog(
          ContainerIdToCleanClusterJ.class);

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
  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public List<HopContainerId> findByRMNode(String rmnodeId) throws
      StorageException {
    LOG.debug("HOP :: ClusterJ ContainerIdToClean.findByRMNode - START:"
            + rmnodeId);
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<ContainerIdToCleanDTO> dobj = qb.createQueryDefinition(
            ContainerIdToCleanDTO.class);
    HopsPredicate pred = dobj.get(RMNODEID).equal(dobj.param(RMNODEID));
    dobj.where(pred);
    HopsQuery<ContainerIdToCleanDTO> query = session.createQuery(dobj);
    query.setParameter(RMNODEID, rmnodeId);
    List<ContainerIdToCleanDTO> results = query.getResultList();
    LOG.debug("HOP :: ClusterJ ContainerIdToClean.findByRMNode - FINISH:"
            + rmnodeId);
    if (results != null && !results.isEmpty()) {
      return createContainersToCleanList(results);
    }
    return null;
  }

  @Override
  public Map<String, Set<HopContainerId>> getAll() throws StorageException {
    LOG.debug("HOP :: ClusterJ ContainerIdToClean.getAll - START");

    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ContainerIdToCleanDTO> dobj
            = qb.createQueryDefinition(
                    ContainerIdToCleanDTO.class);
    HopsQuery<ContainerIdToCleanDTO> query = session.
            createQuery(dobj);
    List<ContainerIdToCleanDTO> results = query.
            getResultList();
    LOG.debug("HOP :: ClusterJ ContainerIdToClean.findByRMNode - FINISH");

    return createMap(results);
  }

  @Override
  public void addAll(Collection<HopContainerId> containers) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<ContainerIdToCleanDTO> toModify
            = new ArrayList<ContainerIdToCleanDTO>();
    for (HopContainerId hop : containers) {
      toModify.add(createPersistable(hop, session));
    }
    session.savePersistentAll(toModify);
    session.flush();
  }

  @Override

  public void removeAll(Collection<HopContainerId> containers) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<ContainerIdToCleanDTO> toRemove
            = new ArrayList<ContainerIdToCleanDTO>();
    for (HopContainerId hop : containers) {
      toRemove.add(createPersistable(hop, session));
    }
    session.deletePersistentAll(toRemove);
    session.flush();
  }

  private ContainerIdToCleanDTO createPersistable(HopContainerId hop,
          HopsSession session) throws StorageException {
    ContainerIdToCleanDTO dto = session.newInstance(ContainerIdToCleanDTO.class);
    //Set values to persist new ContainerStatus
    dto.setrmnodeid(hop.getRmnodeid());
    dto.setcontainerid(hop.getContainerId());
    return dto;
  }

  private HopContainerId createHopContainerIdToClean(ContainerIdToCleanDTO dto) {
    HopContainerId hop = new HopContainerId(dto.getrmnodeid(), dto.
            getcontainerid());
    return hop;
  }

  private List<HopContainerId> createContainersToCleanList(
          List<ContainerIdToCleanDTO> results) {
    List<HopContainerId> containersToClean = new ArrayList<HopContainerId>();
    for (ContainerIdToCleanDTO persistable : results) {
      containersToClean.add(createHopContainerIdToClean(persistable));
    }
    return containersToClean;
  }

  private Map<String, Set<HopContainerId>> createMap(
          List<ContainerIdToCleanDTO> results) {
    Map<String, Set<HopContainerId>> map
            = new HashMap<String, Set<HopContainerId>>();
    for (ContainerIdToCleanDTO dto : results) {
      HopContainerId hop
              = createHopContainerIdToClean(dto);
      if (map.get(hop.getRmnodeid()) == null) {
        map.put(hop.getRmnodeid(),
                new HashSet<HopContainerId>());
      }
      map.get(hop.getRmnodeid()).add(hop);
    }
    return map;
  }
}
