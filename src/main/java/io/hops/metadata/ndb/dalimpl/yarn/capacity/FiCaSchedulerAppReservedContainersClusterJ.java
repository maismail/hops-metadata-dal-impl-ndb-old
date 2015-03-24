package io.hops.metadata.ndb.dalimpl.yarn.capacity;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.dal.capacity.FiCaSchedulerAppReservedContainersDataAccess;
import io.hops.metadata.yarn.entity.capacity.FiCaSchedulerAppReservedContainers;
import io.hops.metadata.yarn.tabledef.capacity.FiCaSchedulerAppReservedContainersTableDef;

import java.util.Collection;

public class FiCaSchedulerAppReservedContainersClusterJ
    implements FiCaSchedulerAppReservedContainersTableDef,
    FiCaSchedulerAppReservedContainersDataAccess<FiCaSchedulerAppReservedContainers> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface FiCaSchedulerAppReservedContainersDTO {

    @PrimaryKey
    @Column(name = SCHEDULERAPP_ID)
    String getschedulerappid();

    void setschedulerappid(String schedulerappid);

    @Column(name = PRIORITY_ID)
    int getpriorityid();

    void setpriorityid(int priorityid);

    @Column(name = NODEID)
    int getnodeid();

    void setnodeid(int nodeid);

    @Column(name = RMCONTAINER_ID)
    String getrmcontainerid();

    void setrmcontainerid(String rmcontainerid);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public FiCaSchedulerAppReservedContainers findById(int id)
      throws StorageException {
    HopsSession session = connector.obtainSession();

    FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO
        fiCaSchedulerAppReservedContainersDTO = null;
    if (session != null) {
      fiCaSchedulerAppReservedContainersDTO = session.find(
          FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO.class,
          id);
    }
    if (fiCaSchedulerAppReservedContainersDTO == null) {
      throw new StorageException("HOP :: Error while retrieving row");
    }

    return createHopFiCaSchedulerAppReservedContainers(
        fiCaSchedulerAppReservedContainersDTO);
  }

  @Override
  public void prepare(Collection<FiCaSchedulerAppReservedContainers> modified,
      Collection<FiCaSchedulerAppReservedContainers> removed)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    try {
      if (removed != null) {
        for (FiCaSchedulerAppReservedContainers hop : removed) {
          FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO
              persistable = session.newInstance(
              FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO.class,
              hop.getSchedulerapp_id());
          session.deletePersistent(persistable);
        }
      }
      if (modified != null) {
        for (FiCaSchedulerAppReservedContainers hop : modified) {
          FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO
              persistable = createPersistable(hop, session);
          session.savePersistent(persistable);
        }
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private FiCaSchedulerAppReservedContainers createHopFiCaSchedulerAppReservedContainers(
      FiCaSchedulerAppReservedContainersDTO fiCaSchedulerAppReservedContainersDTO) {
    return new FiCaSchedulerAppReservedContainers(
        fiCaSchedulerAppReservedContainersDTO.getschedulerappid(),
        fiCaSchedulerAppReservedContainersDTO.getpriorityid(),
        fiCaSchedulerAppReservedContainersDTO.getnodeid(),
        fiCaSchedulerAppReservedContainersDTO.getrmcontainerid());
  }

  private FiCaSchedulerAppReservedContainersDTO createPersistable(
      FiCaSchedulerAppReservedContainers hop, HopsSession session)
      throws StorageException {
    FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO
        fiCaSchedulerAppReservedContainersDTO = session.newInstance(
        FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO.class);

    fiCaSchedulerAppReservedContainersDTO
        .setschedulerappid(hop.getSchedulerapp_id());
    fiCaSchedulerAppReservedContainersDTO.setpriorityid(hop.getPriority_id());
    fiCaSchedulerAppReservedContainersDTO.setnodeid(hop.getNodeid());
    fiCaSchedulerAppReservedContainersDTO
        .setrmcontainerid(hop.getRmcontainer_id());

    return fiCaSchedulerAppReservedContainersDTO;
  }

}

