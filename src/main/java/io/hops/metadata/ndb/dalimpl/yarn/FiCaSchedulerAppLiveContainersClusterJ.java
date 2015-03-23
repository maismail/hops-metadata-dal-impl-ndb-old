

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
import io.hops.metadata.yarn.entity.HopFiCaSchedulerAppLiveContainers;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppLiveContainersDataAccess;

public class FiCaSchedulerAppLiveContainersClusterJ implements
    FiCaSchedulerAppLiveContainersTableDef, FiCaSchedulerAppLiveContainersDataAccess<HopFiCaSchedulerAppLiveContainers>{

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
  public Map<String, List<HopFiCaSchedulerAppLiveContainers>> getAll() throws
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
  public void addAll(Collection<HopFiCaSchedulerAppLiveContainers> toAdd) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<FiCaSchedulerAppLiveContainersDTO> toPersist
            = new ArrayList<FiCaSchedulerAppLiveContainersDTO>();
    for (HopFiCaSchedulerAppLiveContainers container : toAdd) {
      FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO persistable
              = createPersistable(container, session);
      toPersist.add(persistable);
    }
    session.savePersistentAll(toPersist);
  }

  @Override
  public void removeAll(Collection<HopFiCaSchedulerAppLiveContainers> toRemove)
          throws StorageException {
    HopsSession session = connector.obtainSession();
    List<FiCaSchedulerAppLiveContainersDTO> toPersist
            = new ArrayList<FiCaSchedulerAppLiveContainersDTO>();
    for (HopFiCaSchedulerAppLiveContainers container : toRemove) {
      Object[] objarr = new Object[2];
      objarr[0] = container.getSchedulerapp_id();
      objarr[1] = container.getRmcontainer_id();
      toPersist.add(session.newInstance(
              FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO.class,
              objarr));
    }
    session.deletePersistentAll(toPersist);
  }

  private HopFiCaSchedulerAppLiveContainers createHopFiCaSchedulerAppLiveContainers(
          FiCaSchedulerAppLiveContainersDTO fiCaSchedulerAppLiveContainersDTO) {
    return new HopFiCaSchedulerAppLiveContainers(
            fiCaSchedulerAppLiveContainersDTO.getschedulerapp_id(),
            fiCaSchedulerAppLiveContainersDTO.getrmcontainerid());
  }

  private FiCaSchedulerAppLiveContainersDTO createPersistable(
          HopFiCaSchedulerAppLiveContainers hop, HopsSession session) throws
          StorageException {
    FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO fiCaSchedulerAppLiveContainersDTO
            = session.newInstance(
                    FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO.class);

    fiCaSchedulerAppLiveContainersDTO.setschedulerapp_id(hop.
            getSchedulerapp_id());
    fiCaSchedulerAppLiveContainersDTO.setrmcontainerid(hop.getRmcontainer_id());

    return fiCaSchedulerAppLiveContainersDTO;
  }

  private List<HopFiCaSchedulerAppLiveContainers> createFiCaSchedulerAppLiveContainersList(
          List<FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO> results) {
    List<HopFiCaSchedulerAppLiveContainers> ficaSchedulerAppLiveContainers
            = new ArrayList<HopFiCaSchedulerAppLiveContainers>();
    for (FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO persistable
            : results) {
      ficaSchedulerAppLiveContainers.add(
              createHopFiCaSchedulerAppLiveContainers(persistable));
    }
    return ficaSchedulerAppLiveContainers;
  }

  private Map<String, List<HopFiCaSchedulerAppLiveContainers>> createMap(
          List<FiCaSchedulerAppLiveContainersDTO> results) {
    Map<String, List<HopFiCaSchedulerAppLiveContainers>> map
            = new HashMap<String, List<HopFiCaSchedulerAppLiveContainers>>();
    for (FiCaSchedulerAppLiveContainersDTO dto : results) {
      HopFiCaSchedulerAppLiveContainers hop
              = createHopFiCaSchedulerAppLiveContainers(dto);
      if (map.get(hop.getSchedulerapp_id()) == null) {
        map.put(hop.getSchedulerapp_id(),
                new ArrayList<HopFiCaSchedulerAppLiveContainers>());
      }
      map.get(hop.getSchedulerapp_id()).add(hop);
    }
    return map;
  }
}
