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
import se.sics.hop.metadata.hdfs.entity.yarn.HopLaunchedContainers;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsPredicate;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.LaunchedContainersDataAccess;
import se.sics.hop.metadata.yarn.tabledef.LaunchedContainersTableDef;

public class LaunchedContainersClusterJ implements LaunchedContainersTableDef, LaunchedContainersDataAccess<HopLaunchedContainers> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface LaunchedContainersDTO {

        @PrimaryKey
        @Column(name = SCHEDULERNODE_ID)
        String getschedulernode_id();

        void setschedulernode_id(String schedulernode_id);

        @PrimaryKey
        @Column(name = CONTAINERID_ID)
        String getcontaineridid();

        void setcontaineridid(String containeridid);

        @Column(name = RMCONTAINER_ID)
        String getrmcontainerid();

        void setrmcontainerid(String rmcontainerid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopLaunchedContainers findEntry(String schedulernodeId, String containeridId) throws StorageException {
        HopsSession session = connector.obtainSession();
        Object[] objarr = new Object[2];
        objarr[0] = schedulernodeId;
        objarr[1] = containeridId;
        LaunchedContainersDTO dto = null;
        if (session != null) {
            dto = session.find(LaunchedContainersDTO.class, objarr);
        }
        if (dto == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createLaunchedContainersEntry(dto);
    }

  @Override
  public Map<String, List<HopLaunchedContainers>> getAll() throws
          StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<LaunchedContainersDTO> dobj
            = qb.createQueryDefinition(
                    LaunchedContainersDTO.class);
    HopsQuery<LaunchedContainersDTO> query = session.
            createQuery(dobj);
    List<LaunchedContainersDTO> results = query.
            getResultList();
    return createMap(results);
  }
    
    
    @Override
    public List<HopLaunchedContainers> findByFiCaSchedulerNode(String schedulernode_id) throws StorageException {
        try {
            HopsSession session = connector.obtainSession();
            HopsQueryBuilder qb = session.getQueryBuilder();

            HopsQueryDomainType<LaunchedContainersDTO> dobj = qb.createQueryDefinition(LaunchedContainersDTO.class);
            HopsPredicate pred1 = dobj.get("schedulernode_id").equal(dobj.param("schedulernode_id"));
            dobj.where(pred1);
            HopsQuery<LaunchedContainersDTO> query = session.createQuery(dobj);
            query.setParameter("schedulernode_id", schedulernode_id);

            List<LaunchedContainersDTO> results = query.getResultList();
            return createLaunchedContainersList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void prepare(Collection<HopLaunchedContainers> modified, Collection<HopLaunchedContainers> removed) throws StorageException {
        HopsSession session = connector.obtainSession();
        try {
            if (removed != null) {
                List<LaunchedContainersDTO> toRemove = new ArrayList<LaunchedContainersDTO>(removed.size());
                for (HopLaunchedContainers hopContainerId : removed) {
                    Object[] objarr = new Object[2];
                    objarr[0] = hopContainerId.getSchedulerNodeID();
                    objarr[1] = hopContainerId.getContainerIdID();
                    toRemove.add(session.newInstance(LaunchedContainersDTO.class, objarr));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<LaunchedContainersDTO> toModify = new ArrayList<LaunchedContainersDTO>(modified.size());
                for (HopLaunchedContainers id : modified) {
                    toModify.add(createPersistable(id, session));
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createLaunchedContainersEntry(HopLaunchedContainers ficaschedulernode, HopLaunchedContainers containerId) throws StorageException {
        HopsSession session = connector.obtainSession();
        createPersistable(ficaschedulernode, session);
    }

    private Map<String, List<HopLaunchedContainers>> createMap( List<LaunchedContainersDTO> dtos){
      Map<String, List<HopLaunchedContainers>> map = new HashMap<String, List<HopLaunchedContainers>>();
      for (LaunchedContainersDTO dto : dtos) {
        HopLaunchedContainers hop = createLaunchedContainersEntry(dto);
        if(map.get(hop.getSchedulerNodeID())==null){
          map.put(hop.getSchedulerNodeID(), new ArrayList<HopLaunchedContainers>());
        }
        map.get(hop.getSchedulerNodeID()).add(hop);
      }
      return map;
    }
    
    private HopLaunchedContainers createLaunchedContainersEntry(LaunchedContainersDTO dto) {
        HopLaunchedContainers hop = new HopLaunchedContainers(
                dto.getschedulernode_id(),
                dto.getcontaineridid(),
                dto.getrmcontainerid());
        return hop;
    }

    private LaunchedContainersDTO createPersistable(HopLaunchedContainers entry, HopsSession session) throws StorageException {
        Object[] objarr = new Object[2];
        objarr[0] = entry.getSchedulerNodeID();
        objarr[1] = entry.getContainerIdID();
        LaunchedContainersDTO persistable = session.newInstance(LaunchedContainersDTO.class, objarr);
        persistable.setschedulernode_id(entry.getSchedulerNodeID());
        persistable.setcontaineridid(entry.getContainerIdID());
        persistable.setrmcontainerid(entry.getRmContainerID());
        return persistable;
    }

    private List<HopLaunchedContainers> createLaunchedContainersList(List<LaunchedContainersDTO> results) {
        List<HopLaunchedContainers> launchedContainers = new ArrayList<HopLaunchedContainers>();
        for (LaunchedContainersDTO persistable : results) {
            launchedContainers.add(createLaunchedContainersEntry(persistable));
        }
        return launchedContainers;
    }
}
