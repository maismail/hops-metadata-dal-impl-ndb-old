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
import se.sics.hop.metadata.hdfs.entity.yarn.HopJustLaunchedContainers;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsPredicate;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.JustLaunchedContainersDataAccess;
import se.sics.hop.metadata.yarn.tabledef.JustLaunchedContainersTableDef;

public class JustLaunchedContainersClusterJ implements JustLaunchedContainersTableDef, JustLaunchedContainersDataAccess<HopJustLaunchedContainers> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface JustLaunchedContainersDTO {

        @PrimaryKey
        @Column(name = RMNODEID)
        String getrmnodeid();

        void setrmnodeid(String rmnodeid);

        @PrimaryKey
        @Column(name = CONTAINERID)
        String getcontainerid();

        void setcontainerid(String containerid);

    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopJustLaunchedContainers findEntry(String rmnodeid, int commandport, int containerid) throws StorageException {
        HopsSession session = connector.obtainSession();
        Object[] objarr = new Object[2];
        objarr[0] = rmnodeid;
        objarr[1] = containerid;
        JustLaunchedContainersDTO container = null;
        if (session != null) {
            container = session.find(JustLaunchedContainersDTO.class, objarr);
        }
        if (container == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createJustLaunchedContainers(container);
    }

    @Override
    public void prepare(Collection<HopJustLaunchedContainers> modified, Collection<HopJustLaunchedContainers> removed) throws StorageException {
        HopsSession session = connector.obtainSession();
        try {
            if (removed != null) {
                List<JustLaunchedContainersDTO> toRemove = new ArrayList<JustLaunchedContainersDTO>(removed.size());
                for (HopJustLaunchedContainers hopContainerId : removed) {
                    Object[] objarr = new Object[2];
                    objarr[0] = hopContainerId.getRmnodeid();
                    objarr[1] = hopContainerId.getContainerId();
                    toRemove.add(session.newInstance(JustLaunchedContainersDTO.class, objarr));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<JustLaunchedContainersDTO> toModify = new ArrayList<JustLaunchedContainersDTO>(modified.size());
                for (HopJustLaunchedContainers hop : modified) {
                    toModify.add(createPersistable(hop, session));
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public List<HopJustLaunchedContainers> findAll() throws StorageException {
        try {
            HopsSession session = connector.obtainSession();
            HopsQueryBuilder qb = session.getQueryBuilder();

            HopsQueryDomainType<JustLaunchedContainersDTO> dobj = qb.createQueryDefinition(JustLaunchedContainersDTO.class);
            HopsQuery<JustLaunchedContainersDTO> query = session.createQuery(dobj);

            List<JustLaunchedContainersDTO> results = query.getResultList();
            return createJustLaunchedContainersList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public List<HopJustLaunchedContainers> findByRMNode(String rmnodeId) throws StorageException {
        try {
            HopsSession session = connector.obtainSession();
            HopsQueryBuilder qb = session.getQueryBuilder();

            HopsQueryDomainType<JustLaunchedContainersDTO> dobj = qb.createQueryDefinition(JustLaunchedContainersDTO.class);
            HopsPredicate pred = dobj.get("rmnodeid").equal(dobj.param("rmnodeid"));
            dobj.where(pred);
            HopsQuery<JustLaunchedContainersDTO> query = session.createQuery(dobj);
            query.setParameter("rmnodeid", rmnodeId);
            List<JustLaunchedContainersDTO> results = query.getResultList();
            return createJustLaunchedContainersList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

  @Override
  public Map<String, List<HopJustLaunchedContainers>> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<JustLaunchedContainersDTO> dobj = qb.createQueryDefinition(
            JustLaunchedContainersDTO.class);
    HopsQuery<JustLaunchedContainersDTO> query = session.createQuery(dobj);

    List<JustLaunchedContainersDTO> results = query.getResultList();
    return createMap(results);
  }

    private HopJustLaunchedContainers createJustLaunchedContainers(JustLaunchedContainersDTO dto) {
        HopJustLaunchedContainers hop = new HopJustLaunchedContainers(dto.getrmnodeid(), dto.getcontainerid());
        return hop;
    }

    /**
     * Persist new map entry.
     *
     * @param entry
     * @param session
     * @return
     */
    private JustLaunchedContainersDTO createPersistable(HopJustLaunchedContainers entry, HopsSession session) throws StorageException {
        JustLaunchedContainersDTO dto = session.newInstance(JustLaunchedContainersDTO.class);
        dto.setcontainerid(entry.getContainerId());
        dto.setrmnodeid(entry.getRmnodeid());
        return dto;
    }

    private List<HopJustLaunchedContainers> createJustLaunchedContainersList(List<JustLaunchedContainersDTO> results) {
        List<HopJustLaunchedContainers> justLaunchedContainers = new ArrayList<HopJustLaunchedContainers>();
        for (JustLaunchedContainersDTO persistable : results) {
            justLaunchedContainers.add(createJustLaunchedContainers(persistable));
        }
        return justLaunchedContainers;
    }
    
  private Map<String, List<HopJustLaunchedContainers>> createMap(
          List<JustLaunchedContainersDTO> results) {
    Map<String, List<HopJustLaunchedContainers>> map
            = new HashMap<String, List<HopJustLaunchedContainers>>();
    for (JustLaunchedContainersDTO persistable : results) {
      HopJustLaunchedContainers hop = createJustLaunchedContainers(persistable);
      if (map.get(hop.getRmnodeid()) == null) {
        map.put(hop.getRmnodeid(), new ArrayList<HopJustLaunchedContainers>());
      }
      map.get(hop.getRmnodeid()).add(hop);
    }
    return map;
  }
}
