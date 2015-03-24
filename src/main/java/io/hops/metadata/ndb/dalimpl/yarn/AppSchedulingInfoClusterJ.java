package io.hops.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.NdbBoolean;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.dal.AppSchedulingInfoDataAccess;
import io.hops.metadata.yarn.entity.AppSchedulingInfo;
import io.hops.metadata.yarn.tabledef.AppSchedulingInfoTableDef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AppSchedulingInfoClusterJ implements AppSchedulingInfoTableDef,
    AppSchedulingInfoDataAccess<AppSchedulingInfo> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface AppSchedulingInfoDTO {

    @Column(name = SCHEDULERAPP_ID)
    String getschedulerapp_id();

    void setschedulerapp_id(String schedulerapp_id);
    
    @PrimaryKey
    @Column(name = APPID)
    String getappid();

    void setappid(String appid);
    
    @Column(name = QUEUENAME)
    String getqueuename();

    void setqueuename(String queuename);

    @Column(name = USER)
    String getuser();

    void setuser(String user);

    @Column(name = CONTAINERIDCOUNTER)
    int getcontaineridcounter();

    void setcontaineridcounter(int containeridcounter);

    @Column(name = PENDING)
    byte getPending();

    void setPending(byte pending);
    
    @Column(name = STOPED)
    byte getStoped();

    void setStoped(byte stoped);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public List<AppSchedulingInfo> findAll()
      throws StorageException, IOException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<AppSchedulingInfoClusterJ.AppSchedulingInfoDTO> dobj =
        qb.createQueryDefinition(
            AppSchedulingInfoClusterJ.AppSchedulingInfoDTO.class);
    HopsQuery<AppSchedulingInfoClusterJ.AppSchedulingInfoDTO> query =
        session.createQuery(dobj);
    List<AppSchedulingInfoClusterJ.AppSchedulingInfoDTO> results =
        query.getResultList();

    return createHopAppSchedulingInfoList(results);

  }

  
  @Override
  public void add(AppSchedulingInfo toAdd) throws StorageException {
    HopsSession session = connector.obtainSession();
    AppSchedulingInfoClusterJ.AppSchedulingInfoDTO persistable =
        createPersistable(toAdd, session);
    session.savePersistent(persistable);
  }
  
  public void remove(AppSchedulingInfo toRemove) throws StorageException {
    HopsSession session = connector.obtainSession();
    session.deletePersistent(session
        .newInstance(AppSchedulingInfoDTO.class, toRemove.getSchedulerAppId()));
  }
  
  
  private List<AppSchedulingInfo> createHopAppSchedulingInfoList(
      List<AppSchedulingInfoClusterJ.AppSchedulingInfoDTO> list)
      throws IOException {
    List<AppSchedulingInfo> queueMetricsList =
        new ArrayList<AppSchedulingInfo>();
    for (AppSchedulingInfoClusterJ.AppSchedulingInfoDTO persistable : list) {
      queueMetricsList.add(createHopAppSchedulingInfo(persistable));
    }
    return queueMetricsList;
  }

  private AppSchedulingInfo createHopAppSchedulingInfo(
      AppSchedulingInfoDTO appSchedulingInfoDTO) {
    return new AppSchedulingInfo(appSchedulingInfoDTO.getschedulerapp_id(),
        appSchedulingInfoDTO.getappid(), appSchedulingInfoDTO.getqueuename(),
        appSchedulingInfoDTO.getuser(),
        appSchedulingInfoDTO.getcontaineridcounter(),
        NdbBoolean.convert(appSchedulingInfoDTO.getPending()),
        NdbBoolean.convert(appSchedulingInfoDTO.getStoped()));
  }

  private AppSchedulingInfoDTO createPersistable(AppSchedulingInfo hop,
      HopsSession session) throws StorageException {
    AppSchedulingInfoClusterJ.AppSchedulingInfoDTO appSchedulingInfoDTO =
        session
            .newInstance(AppSchedulingInfoClusterJ.AppSchedulingInfoDTO.class);

    appSchedulingInfoDTO.setschedulerapp_id(hop.getSchedulerAppId());
    appSchedulingInfoDTO.setappid(hop.getAppId());
    appSchedulingInfoDTO.setcontaineridcounter(hop.getContaineridcounter());
    appSchedulingInfoDTO.setqueuename(hop.getQueuename());
    appSchedulingInfoDTO.setuser(hop.getUser());
    appSchedulingInfoDTO.setPending(NdbBoolean.convert(hop.isPending()));
    appSchedulingInfoDTO.setStoped(NdbBoolean.convert(hop.isStoped()));
    
    return appSchedulingInfoDTO;
  }

}
