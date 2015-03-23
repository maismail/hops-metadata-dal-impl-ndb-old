

package io.hops.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.hops.exception.StorageException;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.tabledef.FiCaSchedulerAppLiveContainersTableDef;
import io.hops.metadata.yarn.entity.FiCaSchedulerAppLiveContainers;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppLiveContainersDataAccess;

public class FiCaSchedulerAppLiveContainersClusterJ implements
    FiCaSchedulerAppLiveContainersTableDef, FiCaSchedulerAppLiveContainersDataAccess<FiCaSchedulerAppLiveContainers>{

    @PersistenceCapable(table = TABLE_NAME)
    public interface FiCaSchedulerAppLiveContainersDTO {

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
  public Map<String, List<FiCaSchedulerAppLiveContainers>> getAll() throws
      StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<FiCaSchedulerAppLiveContainersDTO> dobj
            = qb.createQueryDefinition(
                    FiCaSchedulerAppLiveContainersDTO.class);
    HopsQuery<FiCaSchedulerAppLiveContainersDTO> query = session.
            createQuery(dobj);
    List<FiCaSchedulerAppLiveContainersDTO> results = query.
            getResultList();
    return createMap(results);
  }

  @Override
  public void addAll(Collection<FiCaSchedulerAppLiveContainers> toAdd) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<FiCaSchedulerAppLiveContainersDTO> toPersist
            = new ArrayList<FiCaSchedulerAppLiveContainersDTO>();
    for (FiCaSchedulerAppLiveContainers container : toAdd) {
      FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO persistable
              = createPersistable(container, session);
      toPersist.add(persistable);
    }
    session.savePersistentAll(toPersist);
  }

  @Override
  public void removeAll(Collection<FiCaSchedulerAppLiveContainers> toRemove)
          throws StorageException {
    HopsSession session = connector.obtainSession();
    List<FiCaSchedulerAppLiveContainersDTO> toPersist
            = new ArrayList<FiCaSchedulerAppLiveContainersDTO>();
    for (FiCaSchedulerAppLiveContainers container : toRemove) {
      Object[] objarr = new Object[2];
      objarr[0] = container.getSchedulerapp_id();
      objarr[1] = container.getRmcontainer_id();
      toPersist.add(session.newInstance(
              FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO.class,
              objarr));
    }
    session.deletePersistentAll(toPersist);
  }

  private FiCaSchedulerAppLiveContainers createHopFiCaSchedulerAppLiveContainers(
          FiCaSchedulerAppLiveContainersDTO fiCaSchedulerAppLiveContainersDTO) {
    return new FiCaSchedulerAppLiveContainers(
            fiCaSchedulerAppLiveContainersDTO.getschedulerapp_id(),
            fiCaSchedulerAppLiveContainersDTO.getrmcontainerid());
  }

  private FiCaSchedulerAppLiveContainersDTO createPersistable(
          FiCaSchedulerAppLiveContainers hop, HopsSession session) throws
          StorageException {
    FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO fiCaSchedulerAppLiveContainersDTO
            = session.newInstance(
                    FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO.class);

    fiCaSchedulerAppLiveContainersDTO.setschedulerapp_id(hop.
            getSchedulerapp_id());
    fiCaSchedulerAppLiveContainersDTO.setrmcontainerid(hop.getRmcontainer_id());

    return fiCaSchedulerAppLiveContainersDTO;
  }

  private List<FiCaSchedulerAppLiveContainers> createFiCaSchedulerAppLiveContainersList(
          List<FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO> results) {
    List<FiCaSchedulerAppLiveContainers> ficaSchedulerAppLiveContainers
            = new ArrayList<FiCaSchedulerAppLiveContainers>();
    for (FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO persistable
            : results) {
      ficaSchedulerAppLiveContainers.add(
              createHopFiCaSchedulerAppLiveContainers(persistable));
    }
    return ficaSchedulerAppLiveContainers;
  }

  private Map<String, List<FiCaSchedulerAppLiveContainers>> createMap(
          List<FiCaSchedulerAppLiveContainersDTO> results) {
    Map<String, List<FiCaSchedulerAppLiveContainers>> map
            = new HashMap<String, List<FiCaSchedulerAppLiveContainers>>();
    for (FiCaSchedulerAppLiveContainersDTO dto : results) {
      FiCaSchedulerAppLiveContainers hop
              = createHopFiCaSchedulerAppLiveContainers(dto);
      if (map.get(hop.getSchedulerapp_id()) == null) {
        map.put(hop.getSchedulerapp_id(),
                new ArrayList<FiCaSchedulerAppLiveContainers>());
      }
      map.get(hop.getSchedulerapp_id()).add(hop);
    }
    return map;
  }
}
