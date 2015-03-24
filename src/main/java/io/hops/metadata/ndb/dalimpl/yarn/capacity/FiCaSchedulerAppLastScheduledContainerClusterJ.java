package io.hops.metadata.ndb.dalimpl.yarn.capacity;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.dal.capacity.FiCaSchedulerAppLastScheduledContainerDataAccess;
import io.hops.metadata.yarn.entity.capacity.FiCaSchedulerAppLastScheduledContainer;
import io.hops.metadata.yarn.tabledef.capacity.FiCaSchedulerAppLastScheduledContainerTableDef;

import java.util.Collection;

public class FiCaSchedulerAppLastScheduledContainerClusterJ
    implements FiCaSchedulerAppLastScheduledContainerTableDef,
    FiCaSchedulerAppLastScheduledContainerDataAccess<FiCaSchedulerAppLastScheduledContainer> {


  @PersistenceCapable(table = TABLE_NAME)
  public interface FiCaSchedulerAppLastScheduledContainerDTO {

    @PrimaryKey
    @Column(name = SCHEDULERAPP_ID)
    String getschedulerappid();

    void setschedulerappid(String schedulerappid);

    @Column(name = PRIORITY_ID)
    int getpriorityid();

    void setpriorityid(int priorityid);

    @Column(name = TIME)
    long gettime();

    void settime(long time);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public FiCaSchedulerAppLastScheduledContainer findById(int id)
      throws StorageException {
    HopsSession session = connector.obtainSession();

    FiCaSchedulerAppLastScheduledContainerClusterJ.FiCaSchedulerAppLastScheduledContainerDTO
        fiCaSchedulerAppLastScheduledContainerDTO = null;
    if (session != null) {
      fiCaSchedulerAppLastScheduledContainerDTO = session.find(
          FiCaSchedulerAppLastScheduledContainerClusterJ.FiCaSchedulerAppLastScheduledContainerDTO.class,
          id);
    }
    if (fiCaSchedulerAppLastScheduledContainerDTO == null) {
      throw new StorageException("HOP :: Error while retrieving row");
    }

    return createHopFiCaSchedulerAppLastScheduledContainer(
        fiCaSchedulerAppLastScheduledContainerDTO);
  }

  @Override
  public void prepare(
      Collection<FiCaSchedulerAppLastScheduledContainer> modified,
      Collection<FiCaSchedulerAppLastScheduledContainer> removed)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    try {
      if (removed != null) {
        for (FiCaSchedulerAppLastScheduledContainer hop : removed) {
          FiCaSchedulerAppLastScheduledContainerClusterJ.FiCaSchedulerAppLastScheduledContainerDTO
              persistable = session.newInstance(
              FiCaSchedulerAppLastScheduledContainerClusterJ.FiCaSchedulerAppLastScheduledContainerDTO.class,
              hop.getSchedulerapp_id());
          session.deletePersistent(persistable);
        }
      }
      if (modified != null) {
        for (FiCaSchedulerAppLastScheduledContainer hop : modified) {
          FiCaSchedulerAppLastScheduledContainerClusterJ.FiCaSchedulerAppLastScheduledContainerDTO
              persistable = createPersistable(hop, session);
          session.savePersistent(persistable);
        }
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private FiCaSchedulerAppLastScheduledContainer createHopFiCaSchedulerAppLastScheduledContainer(
      FiCaSchedulerAppLastScheduledContainerDTO fiCaSchedulerAppLastScheduledContainerDTO) {
    return new FiCaSchedulerAppLastScheduledContainer(
        fiCaSchedulerAppLastScheduledContainerDTO.getschedulerappid(),
        fiCaSchedulerAppLastScheduledContainerDTO.getpriorityid(),
        fiCaSchedulerAppLastScheduledContainerDTO.gettime());
  }

  private FiCaSchedulerAppLastScheduledContainerDTO createPersistable(
      FiCaSchedulerAppLastScheduledContainer hop, HopsSession session)
      throws StorageException {
    FiCaSchedulerAppLastScheduledContainerClusterJ.FiCaSchedulerAppLastScheduledContainerDTO
        fiCaSchedulerAppLastScheduledContainerDTO = session.newInstance(
        FiCaSchedulerAppLastScheduledContainerClusterJ.FiCaSchedulerAppLastScheduledContainerDTO.class);

    fiCaSchedulerAppLastScheduledContainerDTO
        .setschedulerappid(hop.getSchedulerapp_id());
    fiCaSchedulerAppLastScheduledContainerDTO
        .setpriorityid(hop.getPriority_id());
    fiCaSchedulerAppLastScheduledContainerDTO.settime(hop.getTime());

    return fiCaSchedulerAppLastScheduledContainerDTO;
  }

}
