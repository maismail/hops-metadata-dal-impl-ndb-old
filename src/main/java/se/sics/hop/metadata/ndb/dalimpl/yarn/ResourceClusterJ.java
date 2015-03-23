package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopResource;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.ResourceDataAccess;
import se.sics.hop.metadata.yarn.tabledef.ResourceTableDef;

public class ResourceClusterJ implements ResourceTableDef, ResourceDataAccess<HopResource> {

  private static final Log LOG = LogFactory.getLog(ResourceClusterJ.class);

  @PersistenceCapable(table = TABLE_NAME)
  public interface ResourceDTO {

    @PrimaryKey
    @Column(name = ID)
    String getId();

    void setId(String id);

    @Column(name = TYPE)
    int getType();

    void setType(int type);

    @Column(name = PARENT)
    int getParent();

    void setParent(int parent);

    @Column(name = MEMORY)
    int getMemory();

    void setMemory(int memory);

    @Column(name = VIRTUAL_CORES)
    int getVirtualcores();

    void setVirtualcores(int virtualcores);
  }
  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public HopResource findEntry(String id, int type, int parent) throws
          StorageException {
    LOG.debug("HOP :: ClusterJ Resource.findEntry - START:" + id);
    HopsSession session = connector.obtainSession();
    if (session != null) {
      ResourceDTO resourceDTO;
      Object[] pk = new Object[3];
      pk[0] = id;
      pk[1] = type;
      pk[2] = parent;
      resourceDTO = session.find(ResourceDTO.class, pk);
      LOG.debug("HOP :: ClusterJ Resource.findEntry - FINISH:" + id);
      if (resourceDTO != null) {
        return createHopResource(resourceDTO);
      }
    }
    return null;
  }

  @Override
  public Map<String, Map<Integer, Map<Integer, HopResource>>> getAll() throws
          StorageException {
    LOG.debug("HOP :: ClusterJ Resource.getAll - START");
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ResourceDTO> dobj
            = qb.createQueryDefinition(
                    ResourceDTO.class);
    HopsQuery<ResourceDTO> query = session.
            createQuery(dobj);
    List<ResourceDTO> results = query.
            getResultList();
    LOG.debug("HOP :: ClusterJ Resource.getAll - FINISH");
    return createMap(results);
  }

  @Override
  public void addAll(Collection<HopResource> toAdd) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<ResourceDTO> toPersist = new ArrayList<ResourceDTO>();
    for (HopResource req : toAdd) {
      toPersist.add(createPersistable(req, session));
    }

    session.savePersistentAll(toPersist);
    session.flush();
  }

  @Override
  public void removeAll(Collection<HopResource> toRemove) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<ResourceDTO> toPersist = new ArrayList<ResourceDTO>();
    for (HopResource req : toRemove) {
      Object[] pk = new Object[3];
      pk[0] = req.getId();
      pk[1] = req.getType();
      pk[2] = req.getParent();
      toPersist.add(session.newInstance(ResourceDTO.class, pk));
    }
    session.deletePersistentAll(toPersist);
    session.flush();
  }

  @Override
  public void add(HopResource resourceNode) throws StorageException {
    HopsSession session = connector.obtainSession();
    session.savePersistent(createPersistable(resourceNode, session));
  }

  private HopResource createHopResource(ResourceDTO resourceDTO) {
    if (resourceDTO == null) {
      return null;

    }
    return new HopResource(resourceDTO.getId(), resourceDTO.getType(),
            resourceDTO.getParent(), resourceDTO.getMemory(), resourceDTO.
            getVirtualcores());
  }

  private ResourceDTO createPersistable(HopResource resource,
          HopsSession session) throws StorageException {
    ResourceDTO resourceDTO = session.newInstance(ResourceDTO.class);
    resourceDTO.setId(resource.getId());
    resourceDTO.setType(resource.getType());
    resourceDTO.setParent(resource.getParent());
    resourceDTO.setMemory(resource.getMemory());
    resourceDTO.setVirtualcores(resource.getVirtualCores());
    return resourceDTO;
  }

  private Map<String, Map<Integer, Map<Integer, HopResource>>> createMap(
          List<ResourceDTO> results) {
    Map<String, Map<Integer, Map<Integer, HopResource>>> map
            = new HashMap<String, Map<Integer, Map<Integer, HopResource>>>();
    for (ResourceDTO dto : results) {
      HopResource hop
              = createHopResource(dto);
      if (map.get(hop.getId()) == null) {
        map.put(hop.getId(),
                new HashMap<Integer, Map<Integer, HopResource>>());
      }
      Map<Integer, Map<Integer, HopResource>> inerMap = map.get(hop.getId());
      if (inerMap.get(hop.getType()) == null) {
        inerMap.put(hop.getType(), new HashMap<Integer, HopResource>());
      }
      inerMap.get(hop.getType()).put(hop.getParent(), hop);
    }
    return map;
  }
}
