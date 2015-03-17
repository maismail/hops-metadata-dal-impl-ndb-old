

package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopSchedulerApplication;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.SchedulerApplicationDataAccess;
import se.sics.hop.metadata.yarn.tabledef.SchedulerApplicationTableDef;

public class SchedulerApplicationClusterJ implements SchedulerApplicationTableDef, SchedulerApplicationDataAccess<HopSchedulerApplication> {


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
    public HopSchedulerApplication findById(String id) throws StorageException {
        HopsSession session = connector.obtainSession();

        SchedulerApplicationClusterJ.SchedulerApplicationDTO schedulerApplicationDTO = null;
        if (session != null) {
            schedulerApplicationDTO = session.find(SchedulerApplicationClusterJ.SchedulerApplicationDTO.class, id);
        }
        if (schedulerApplicationDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopSchedulerApplication(schedulerApplicationDTO);
    }
    
    @Override
    public Map<String, HopSchedulerApplication> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
      HopsQueryBuilder qb = session.getQueryBuilder();
      HopsQueryDomainType<SchedulerApplicationClusterJ.SchedulerApplicationDTO> dobj
            = qb.createQueryDefinition(
                    SchedulerApplicationClusterJ.SchedulerApplicationDTO.class);
      HopsQuery<SchedulerApplicationClusterJ.SchedulerApplicationDTO> query = session.
            createQuery(dobj);
    List<SchedulerApplicationClusterJ.SchedulerApplicationDTO> results = query.
            getResultList();
    return createApplicationIdMap(results);
  }
    

  private Map<String, HopSchedulerApplication> createApplicationIdMap(
          List<SchedulerApplicationClusterJ.SchedulerApplicationDTO> list)
          {
    Map<String, HopSchedulerApplication> schedulerApplications
            = new HashMap<String, HopSchedulerApplication>();
    for (SchedulerApplicationClusterJ.SchedulerApplicationDTO persistable : list) {
      HopSchedulerApplication app = createHopSchedulerApplication(persistable);
      schedulerApplications.put(app.getAppid(), app);
    }
    return schedulerApplications;
  }
    
    @Override
    public void prepare(Collection<HopSchedulerApplication> modified, Collection<HopSchedulerApplication> removed) throws StorageException {
        HopsSession session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopSchedulerApplication hop : removed) {
                    SchedulerApplicationClusterJ.SchedulerApplicationDTO persistable = session.newInstance(SchedulerApplicationClusterJ.SchedulerApplicationDTO.class, hop.getAppid());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopSchedulerApplication hop : modified) {
                    SchedulerApplicationClusterJ.SchedulerApplicationDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    private HopSchedulerApplication createHopSchedulerApplication(SchedulerApplicationDTO schedulerApplicationDTO) {
        return new HopSchedulerApplication(schedulerApplicationDTO.getappid(), schedulerApplicationDTO.getuser(), schedulerApplicationDTO.getqueuename());
    }

    private SchedulerApplicationDTO createPersistable(HopSchedulerApplication hop, HopsSession session) throws StorageException {
        SchedulerApplicationClusterJ.SchedulerApplicationDTO schedulerApplicationDTO = session.newInstance(SchedulerApplicationClusterJ.SchedulerApplicationDTO.class);
        
        schedulerApplicationDTO.setappid(hop.getAppid());
        schedulerApplicationDTO.setuser(hop.getUser());
        schedulerApplicationDTO.setqueuename(hop.getQueuename());
        return schedulerApplicationDTO;
    }
    
    @Override
    public void createEntry(HopSchedulerApplication SchedulerApp) throws StorageException {
        HopsSession session = connector.obtainSession();
        session.savePersistent(createPersistable(SchedulerApp, session));
    }   
}
