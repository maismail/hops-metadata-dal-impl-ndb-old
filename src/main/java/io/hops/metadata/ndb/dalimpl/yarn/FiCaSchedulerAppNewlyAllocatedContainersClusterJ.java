

package io.hops.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.hops.exception.StorageException;
import io.hops.metadata.yarn.entity.HopFiCaSchedulerAppNewlyAllocatedContainers;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppNewlyAllocatedContainersDataAccess;
import io.hops.metadata.yarn.tabledef.FiCaSchedulerAppNewlyAllocatedContainersTableDef;

public class FiCaSchedulerAppNewlyAllocatedContainersClusterJ implements
    FiCaSchedulerAppNewlyAllocatedContainersTableDef,
    FiCaSchedulerAppNewlyAllocatedContainersDataAccess<HopFiCaSchedulerAppNewlyAllocatedContainers> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface FiCaSchedulerAppNewlyAllocatedContainersDTO {

    @PrimaryKey
    @Column(name = SCHEDULERAPP_ID)
    String getschedulerapp_id();

    void setschedulerapp_id(String schedulerapp_id);

    @PrimaryKey
    @Column(name = RMCONTAINER_ID)
    String getrmcontainerid();

    void setrmcontainerid(String rmcontainerid);
  }
  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public List<HopFiCaSchedulerAppNewlyAllocatedContainers> findById(
          String ficaId) throws StorageException {

    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<FiCaSchedulerAppNewlyAllocatedContainersDTO> dobj
            = qb.createQueryDefinition(
                    FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO.class);
    HopsPredicate pred1 = dobj.get("schedulerapp_id").equal(dobj.param(
            "schedulerapp_id"));
    dobj.where(pred1);
    HopsQuery<FiCaSchedulerAppNewlyAllocatedContainersDTO> query
            = session.createQuery(dobj);
    query.setParameter("schedulerapp_id", ficaId);

    List<FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO> results
            = query.getResultList();
    return createFiCaSchedulerAppNewlyAllocatedContainersList(results);

  }

  @Override
  public Map<String, List<HopFiCaSchedulerAppNewlyAllocatedContainers>> getAll()
          throws IOException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<FiCaSchedulerAppNewlyAllocatedContainersDTO> dobj
            = qb.createQueryDefinition(
                    FiCaSchedulerAppNewlyAllocatedContainersDTO.class);
    HopsQuery<FiCaSchedulerAppNewlyAllocatedContainersDTO> query = session.
            createQuery(dobj);
    List<FiCaSchedulerAppNewlyAllocatedContainersDTO> results = query.
            getResultList();
    return createMap(results);
  }

  @Override
  public void addAll(
          Collection<HopFiCaSchedulerAppNewlyAllocatedContainers> toAdd) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<FiCaSchedulerAppNewlyAllocatedContainersDTO> toPersist
            = new ArrayList<FiCaSchedulerAppNewlyAllocatedContainersDTO>();
    for (HopFiCaSchedulerAppNewlyAllocatedContainers hop : toAdd) {
      FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO persistable
              = createPersistable(hop, session);
      toPersist.add(persistable);
    }
    session.savePersistentAll(toPersist);
  }

  @Override
  public void removeAll(
          Collection<HopFiCaSchedulerAppNewlyAllocatedContainers> toRemove)
          throws StorageException {
    HopsSession session = connector.obtainSession();
    List<FiCaSchedulerAppNewlyAllocatedContainersDTO> toPersist
            = new ArrayList<FiCaSchedulerAppNewlyAllocatedContainersDTO>();
    for (HopFiCaSchedulerAppNewlyAllocatedContainers hop : toRemove) {

      Object[] objarr = new Object[2];
      objarr[0] = hop.getSchedulerapp_id();
      objarr[1] = hop.getRmcontainer_id();
      toPersist.add(session.newInstance(
              FiCaSchedulerAppNewlyAllocatedContainersDTO.class, objarr));
    }
    session.deletePersistentAll(toPersist);
  }

  private HopFiCaSchedulerAppNewlyAllocatedContainers createHopFiCaSchedulerAppNewlyAllocatedContainers(
          FiCaSchedulerAppNewlyAllocatedContainersDTO fiCaSchedulerAppNewlyAllocatedContainersDTO) {
    return new HopFiCaSchedulerAppNewlyAllocatedContainers(
            fiCaSchedulerAppNewlyAllocatedContainersDTO.getschedulerapp_id(),
            fiCaSchedulerAppNewlyAllocatedContainersDTO.getrmcontainerid());
  }

  private FiCaSchedulerAppNewlyAllocatedContainersDTO createPersistable(
          HopFiCaSchedulerAppNewlyAllocatedContainers hop, HopsSession session)
          throws StorageException {
    FiCaSchedulerAppNewlyAllocatedContainersDTO fiCaSchedulerAppNewlyAllocatedContainersDTO
            = session.newInstance(
                    FiCaSchedulerAppNewlyAllocatedContainersDTO.class);

    fiCaSchedulerAppNewlyAllocatedContainersDTO.setschedulerapp_id(hop.
            getSchedulerapp_id());
    fiCaSchedulerAppNewlyAllocatedContainersDTO.setrmcontainerid(hop.
            getRmcontainer_id());

    return fiCaSchedulerAppNewlyAllocatedContainersDTO;
  }

  private List<HopFiCaSchedulerAppNewlyAllocatedContainers> createFiCaSchedulerAppNewlyAllocatedContainersList(
          List<FiCaSchedulerAppNewlyAllocatedContainersDTO> results) {
    List<HopFiCaSchedulerAppNewlyAllocatedContainers> ficaSchedulerAppNewlyAllocatedContainers
            = new ArrayList<HopFiCaSchedulerAppNewlyAllocatedContainers>();
    for (FiCaSchedulerAppNewlyAllocatedContainersDTO persistable
            : results) {
      ficaSchedulerAppNewlyAllocatedContainers.add(
              createHopFiCaSchedulerAppNewlyAllocatedContainers(persistable));
    }
    return ficaSchedulerAppNewlyAllocatedContainers;
  }

  private Map<String, List<HopFiCaSchedulerAppNewlyAllocatedContainers>>
          createMap(List<FiCaSchedulerAppNewlyAllocatedContainersDTO> results) {
    Map<String, List<HopFiCaSchedulerAppNewlyAllocatedContainers>> map
            = new HashMap<String, List<HopFiCaSchedulerAppNewlyAllocatedContainers>>();
    for (FiCaSchedulerAppNewlyAllocatedContainersDTO persistable : results) {
      HopFiCaSchedulerAppNewlyAllocatedContainers hop
              = createHopFiCaSchedulerAppNewlyAllocatedContainers(persistable);
      if (map.get(hop.getSchedulerapp_id()) == null) {
        map.put(hop.getSchedulerapp_id(),
                new ArrayList<HopFiCaSchedulerAppNewlyAllocatedContainers>());
      }
      map.get(hop.getSchedulerapp_id()).add(hop);
    }
    return map;
  }
}
