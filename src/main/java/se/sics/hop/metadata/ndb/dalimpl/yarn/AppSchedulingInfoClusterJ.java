/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopAppSchedulingInfo;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.NdbBoolean;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.AppSchedulingInfoDataAccess;
import se.sics.hop.metadata.yarn.tabledef.AppSchedulingInfoTableDef;
import static se.sics.hop.metadata.yarn.tabledef.AppSchedulingInfoTableDef.PENDING;

/**
 *
 * @author nickstanogias
 */
public class AppSchedulingInfoClusterJ implements AppSchedulingInfoTableDef, AppSchedulingInfoDataAccess<HopAppSchedulingInfo> {

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
  public HopAppSchedulingInfo findById(String id) throws StorageException {
    HopsSession session = connector.obtainSession();

    AppSchedulingInfoClusterJ.AppSchedulingInfoDTO appSchedulingInfoDTO = null;
    if (session != null) {
      appSchedulingInfoDTO = session.find(AppSchedulingInfoClusterJ.AppSchedulingInfoDTO.class, id);
    }
    if (appSchedulingInfoDTO == null) {
      throw new StorageException("HOP :: Error while retrieving row");
    }

    return createHopAppSchedulingInfo(appSchedulingInfoDTO);
  }

  @Override
  public List<HopAppSchedulingInfo> findAll() throws StorageException, IOException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<AppSchedulingInfoClusterJ.AppSchedulingInfoDTO> dobj = qb.createQueryDefinition(AppSchedulingInfoClusterJ.AppSchedulingInfoDTO.class);
    HopsQuery<AppSchedulingInfoClusterJ.AppSchedulingInfoDTO> query = session.createQuery(dobj);
    List<AppSchedulingInfoClusterJ.AppSchedulingInfoDTO> results = query.getResultList();

    return createHopAppSchedulingInfoList(results);

  }

  private List<HopAppSchedulingInfo> createHopAppSchedulingInfoList(List<AppSchedulingInfoClusterJ.AppSchedulingInfoDTO> list) throws IOException {
    List<HopAppSchedulingInfo> queueMetricsList = new ArrayList<HopAppSchedulingInfo>();
    for (AppSchedulingInfoClusterJ.AppSchedulingInfoDTO persistable : list) {
      queueMetricsList.add(createHopAppSchedulingInfo(persistable));
    }
    return queueMetricsList;
  }

  @Override
  public void prepare(Collection<HopAppSchedulingInfo> modified, Collection<HopAppSchedulingInfo> removed) throws StorageException {
    HopsSession session = connector.obtainSession();
    try {
      if (removed != null) {
        for (HopAppSchedulingInfo hop : removed) {
          AppSchedulingInfoClusterJ.AppSchedulingInfoDTO persistable = 
                  session.newInstance(AppSchedulingInfoClusterJ.AppSchedulingInfoDTO.class, 
                          hop.getAppId());
          session.deletePersistent(persistable);
        }
      }
      if (modified != null) {
        for (HopAppSchedulingInfo hop : modified) {
          AppSchedulingInfoClusterJ.AppSchedulingInfoDTO persistable = createPersistable(hop, session);
          session.savePersistent(persistable);
        }
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private HopAppSchedulingInfo createHopAppSchedulingInfo(AppSchedulingInfoDTO appSchedulingInfoDTO) {
    return new HopAppSchedulingInfo(appSchedulingInfoDTO.getschedulerapp_id(), appSchedulingInfoDTO.getappid(),
            appSchedulingInfoDTO.getqueuename(),
            appSchedulingInfoDTO.getuser(),
            appSchedulingInfoDTO.getcontaineridcounter(),
            NdbBoolean.convert(appSchedulingInfoDTO.getPending()),
            NdbBoolean.convert(appSchedulingInfoDTO.getStoped()));
  }

  private AppSchedulingInfoDTO createPersistable(HopAppSchedulingInfo hop, HopsSession session) throws StorageException {
    AppSchedulingInfoClusterJ.AppSchedulingInfoDTO appSchedulingInfoDTO = 
            session.newInstance(AppSchedulingInfoClusterJ.AppSchedulingInfoDTO.class);

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
