

package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopFiCaSchedulerAppNewlyAllocatedContainers;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsPredicate;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.FiCaSchedulerAppNewlyAllocatedContainersDataAccess;
import se.sics.hop.metadata.yarn.tabledef.FiCaSchedulerAppNewlyAllocatedContainersTableDef;

public class FiCaSchedulerAppNewlyAllocatedContainersClusterJ implements FiCaSchedulerAppNewlyAllocatedContainersTableDef, FiCaSchedulerAppNewlyAllocatedContainersDataAccess<HopFiCaSchedulerAppNewlyAllocatedContainers> {


    @PersistenceCapable(table = TABLE_NAME)
    public interface FiCaSchedulerAppNewlyAllocatedContainersDTO {

        @PrimaryKey
        @Column(name = SCHEDULERAPP_ID)
        String getschedulerapp_id();
        void setschedulerapp_id(String schedulerapp_id);

        @Column(name = RMCONTAINER_ID)
        String getrmcontainerid();
        void setrmcontainerid(String rmcontainerid);    
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public List<HopFiCaSchedulerAppNewlyAllocatedContainers> findById(String ficaId) throws StorageException {
        
            HopsSession session = connector.obtainSession();
            HopsQueryBuilder qb = session.getQueryBuilder();

            HopsQueryDomainType<FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO> dobj = qb.createQueryDefinition(FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO.class);
            HopsPredicate pred1 = dobj.get("schedulerapp_id").equal(dobj.param("schedulerapp_id"));
            dobj.where(pred1);
            HopsQuery<FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO> query = session.createQuery(dobj);
            query.setParameter("schedulerapp_id", ficaId);

            List<FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO> results = query.getResultList();
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
    public void prepare(Collection<HopFiCaSchedulerAppNewlyAllocatedContainers> modified, Collection<HopFiCaSchedulerAppNewlyAllocatedContainers> removed) throws StorageException {
        HopsSession session = connector.obtainSession();
        try {
            if (removed != null) {
                List<FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO> toRemove = new ArrayList<FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO>();
                for (HopFiCaSchedulerAppNewlyAllocatedContainers hop : removed) {
                    Object[] objarr = new Object[2];
                    objarr[0] = hop.getSchedulerapp_id();
                    objarr[1] = hop.getRmcontainer_id();
                    toRemove.add(session.newInstance(FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO.class, objarr));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                for (HopFiCaSchedulerAppNewlyAllocatedContainers hop : modified) {
                    FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }   
    }
    
    private HopFiCaSchedulerAppNewlyAllocatedContainers createHopFiCaSchedulerAppNewlyAllocatedContainers(FiCaSchedulerAppNewlyAllocatedContainersDTO fiCaSchedulerAppNewlyAllocatedContainersDTO) {
        return new HopFiCaSchedulerAppNewlyAllocatedContainers(fiCaSchedulerAppNewlyAllocatedContainersDTO.getschedulerapp_id(),
                                                               fiCaSchedulerAppNewlyAllocatedContainersDTO.getrmcontainerid());
    }

    private FiCaSchedulerAppNewlyAllocatedContainersDTO createPersistable(HopFiCaSchedulerAppNewlyAllocatedContainers hop, HopsSession session) throws StorageException {
        FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO fiCaSchedulerAppNewlyAllocatedContainersDTO = session.newInstance(FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO.class);
        
        fiCaSchedulerAppNewlyAllocatedContainersDTO.setschedulerapp_id(hop.getSchedulerapp_id());
        fiCaSchedulerAppNewlyAllocatedContainersDTO.setrmcontainerid(hop.getRmcontainer_id());
        
        return fiCaSchedulerAppNewlyAllocatedContainersDTO;
    }
    
    private List<HopFiCaSchedulerAppNewlyAllocatedContainers> createFiCaSchedulerAppNewlyAllocatedContainersList(List<FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO> results) {
        List<HopFiCaSchedulerAppNewlyAllocatedContainers> ficaSchedulerAppNewlyAllocatedContainers = new ArrayList<HopFiCaSchedulerAppNewlyAllocatedContainers>();
        for (FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO persistable : results) {
            ficaSchedulerAppNewlyAllocatedContainers.add(createHopFiCaSchedulerAppNewlyAllocatedContainers(persistable));
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
