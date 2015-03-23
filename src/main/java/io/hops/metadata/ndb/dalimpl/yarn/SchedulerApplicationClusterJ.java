

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
import io.hops.metadata.yarn.entity.HopSchedulerApplication;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.tabledef.SchedulerApplicationTableDef;
import io.hops.metadata.yarn.dal.SchedulerApplicationDataAccess;

public class SchedulerApplicationClusterJ implements
    SchedulerApplicationTableDef, SchedulerApplicationDataAccess<HopSchedulerApplication> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface SchedulerApplicationDTO {

    @PrimaryKey
    @Column(name = APPID)
    String getappid();

    void setappid(String appid);

    @Column(name = USER)
    String getuser();

    void setuser(String user);

    @Column(name = QUEUENAME)
    String getqueuename();

    void setqueuename(String queuename);

  }
  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public Map<String, HopSchedulerApplication> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<SchedulerApplicationDTO> dobj
            = qb.createQueryDefinition(
                    SchedulerApplicationClusterJ.SchedulerApplicationDTO.class);
    HopsQuery<SchedulerApplicationDTO> query
            = session.
            createQuery(dobj);
    List<SchedulerApplicationClusterJ.SchedulerApplicationDTO> results = query.
            getResultList();
    return createApplicationIdMap(results);
  }

  @Override
  public void addAll(Collection<HopSchedulerApplication> toAdd) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<SchedulerApplicationDTO> toPersist
            = new ArrayList<SchedulerApplicationDTO>();
    for (HopSchedulerApplication req : toAdd) {
      toPersist.add(createPersistable(req, session));
    }
    session.savePersistentAll(toPersist);
  }

  @Override
  public void removeAll(Collection<HopSchedulerApplication> toRemove) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<SchedulerApplicationDTO> toPersist
            = new ArrayList<SchedulerApplicationDTO>();
    for (HopSchedulerApplication entry : toRemove) {
      toPersist.add(session.newInstance(SchedulerApplicationDTO.class, entry.
              getAppid()));
    }
    session.deletePersistentAll(toPersist);
  }

  private Map<String, HopSchedulerApplication> createApplicationIdMap(
          List<SchedulerApplicationClusterJ.SchedulerApplicationDTO> list) {
    Map<String, HopSchedulerApplication> schedulerApplications
            = new HashMap<String, HopSchedulerApplication>();
    for (SchedulerApplicationClusterJ.SchedulerApplicationDTO persistable : list) {
      HopSchedulerApplication app = createHopSchedulerApplication(persistable);
      schedulerApplications.put(app.getAppid(), app);
    }
    return schedulerApplications;
  }

  private HopSchedulerApplication createHopSchedulerApplication(
          SchedulerApplicationDTO schedulerApplicationDTO) {
    return new HopSchedulerApplication(schedulerApplicationDTO.getappid(),
            schedulerApplicationDTO.getuser(), schedulerApplicationDTO.
            getqueuename());
  }

  private SchedulerApplicationDTO createPersistable(HopSchedulerApplication hop,
          HopsSession session) throws StorageException {
    SchedulerApplicationClusterJ.SchedulerApplicationDTO schedulerApplicationDTO
            = session.newInstance(
                    SchedulerApplicationClusterJ.SchedulerApplicationDTO.class);

    schedulerApplicationDTO.setappid(hop.getAppid());
    schedulerApplicationDTO.setuser(hop.getUser());
    schedulerApplicationDTO.setqueuename(hop.getQueuename());
    return schedulerApplicationDTO;
  }

}
