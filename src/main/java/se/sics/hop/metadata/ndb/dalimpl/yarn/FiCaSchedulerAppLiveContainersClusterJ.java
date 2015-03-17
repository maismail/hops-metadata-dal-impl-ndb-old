

package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopFiCaSchedulerAppLiveContainers;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsPredicate;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.FiCaSchedulerAppLiveContainersDataAccess;
import se.sics.hop.metadata.yarn.tabledef.FiCaSchedulerAppLiveContainersTableDef;

public class FiCaSchedulerAppLiveContainersClusterJ implements FiCaSchedulerAppLiveContainersTableDef, FiCaSchedulerAppLiveContainersDataAccess<HopFiCaSchedulerAppLiveContainers>{

    @PersistenceCapable(table = TABLE_NAME)
    public interface FiCaSchedulerAppLiveContainersDTO {

        @PrimaryKey
        @Column(name = SCHEDULERAPP_ID)
        String getschedulerapp_id();
        void setschedulerapp_id(String schedulerapp_id);

        @Column(name = CONTAINERID_ID)
        String getcontaineridid();
        void setcontaineridid(String containeridid);
        
        @Column(name = RMCONTAINER_ID)
        String getrmcontainerid();
        void setrmcontainerid(String rmcontainerid);
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public List<HopFiCaSchedulerAppLiveContainers> findById(String ficaId) throws StorageException {
        try {
            HopsSession session = connector.obtainSession();
            HopsQueryBuilder qb = session.getQueryBuilder();

            HopsQueryDomainType<FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO> dobj = qb.createQueryDefinition(FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO.class);
            HopsPredicate pred1 = dobj.get("schedulerapp_id").equal(dobj.param("schedulerapp_id"));
            dobj.where(pred1);
            HopsQuery<FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO> query = session.createQuery(dobj);
            query.setParameter("schedulerapp_id", ficaId);

            List<FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO> results = query.getResultList();
            return createFiCaSchedulerAppLiveContainersList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

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
    public void prepare(Collection<HopFiCaSchedulerAppLiveContainers> modified, Collection<HopFiCaSchedulerAppLiveContainers> removed) throws StorageException {
        HopsSession session = connector.obtainSession();
        try {
            if (removed != null) {
                List<FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO> toRemove = new ArrayList<FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO>();
                for (HopFiCaSchedulerAppLiveContainers hop : removed) {
                    Object[] objarr = new Object[2];
                    objarr[0] = hop.getSchedulerapp_id();
                    objarr[1] = hop.getContainerid_id();
                    toRemove.add(session.newInstance(FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO.class, objarr));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                for (HopFiCaSchedulerAppLiveContainers hop : modified) {
                    FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    private HopFiCaSchedulerAppLiveContainers createHopFiCaSchedulerAppLiveContainers(FiCaSchedulerAppLiveContainersDTO fiCaSchedulerAppLiveContainersDTO) {
        return new HopFiCaSchedulerAppLiveContainers(fiCaSchedulerAppLiveContainersDTO.getschedulerapp_id(),
                                                    fiCaSchedulerAppLiveContainersDTO.getcontaineridid(),
                                                    fiCaSchedulerAppLiveContainersDTO.getrmcontainerid());
    }

    private FiCaSchedulerAppLiveContainersDTO createPersistable(HopFiCaSchedulerAppLiveContainers hop, HopsSession session) throws StorageException {
        FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO fiCaSchedulerAppLiveContainersDTO = session.newInstance(FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO.class);
        
        fiCaSchedulerAppLiveContainersDTO.setschedulerapp_id(hop.getSchedulerapp_id());
        fiCaSchedulerAppLiveContainersDTO.setcontaineridid(hop.getContainerid_id());
        fiCaSchedulerAppLiveContainersDTO.setrmcontainerid(hop.getRmcontainer_id());
        
        return fiCaSchedulerAppLiveContainersDTO;
    }
    
    private List<HopFiCaSchedulerAppLiveContainers> createFiCaSchedulerAppLiveContainersList(List<FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO> results) {
        List<HopFiCaSchedulerAppLiveContainers> ficaSchedulerAppLiveContainers = new ArrayList<HopFiCaSchedulerAppLiveContainers>();
        for (FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO persistable : results) {
            ficaSchedulerAppLiveContainers.add(createHopFiCaSchedulerAppLiveContainers(persistable));
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
