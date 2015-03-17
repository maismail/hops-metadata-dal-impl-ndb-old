

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
import java.util.zip.DataFormatException;

import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopResourceRequest;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsPredicate;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.ResourceRequestDataAccess;
import se.sics.hop.metadata.yarn.tabledef.ResourceRequestTableDef;
import se.sics.hop.util.CompressionUtils;

public class ResourceRequestClusterJ implements ResourceRequestTableDef, ResourceRequestDataAccess<HopResourceRequest> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface ResourceRequestDTO {

        @PrimaryKey
        @Column(name = APPSCHEDULINGINFO_ID)
        String getappschedulinginfo_id();
        void setappschedulinginfo_id(String appschedulinginfo_id);

        @Column(name = PRIORITY)
        int getpriority();
        void setpriority(int priority);
        
        @Column(name = NAME)
        String getname();
        void setname(String name);

        @Column(name = RESOURCEREQUESTSTATE)
        byte[] getresourcerequeststate();
        void setresourcerequeststate(byte[] resourcerequeststate);
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public List<HopResourceRequest> findById(String id) throws StorageException {
        try {
            HopsSession session = connector.obtainSession();
            HopsQueryBuilder qb = session.getQueryBuilder();

            HopsQueryDomainType<ResourceRequestClusterJ.ResourceRequestDTO> dobj = qb.createQueryDefinition(ResourceRequestClusterJ.ResourceRequestDTO.class);
            HopsPredicate pred1 = dobj.get("appschedulinginfo_id").equal(dobj.param("appschedulinginfo_id"));
            dobj.where(pred1);
            HopsQuery<ResourceRequestClusterJ.ResourceRequestDTO> query = session.createQuery(dobj);
            query.setParameter("appschedulinginfo_id", id);

            List<ResourceRequestClusterJ.ResourceRequestDTO> results = query.getResultList();
            return createResourceRequestList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public Map<String, List<HopResourceRequest>>  getAll() throws StorageException{
      HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ResourceRequestDTO> dobj
            = qb.createQueryDefinition(
                    ResourceRequestDTO.class);
    HopsQuery<ResourceRequestDTO> query = session.
            createQuery(dobj);
    List<ResourceRequestDTO> results = query.
            getResultList();
    return createMap(results);
    }
    
    @Override
    public void prepare(Collection<HopResourceRequest> modified, Collection<HopResourceRequest> removed) throws StorageException {
        HopsSession session = connector.obtainSession();
        try {
            if (removed != null) {
                List<ResourceRequestClusterJ.ResourceRequestDTO> toRemove = new ArrayList<ResourceRequestClusterJ.ResourceRequestDTO>();
                for (HopResourceRequest hop : removed) {
                    Object[] objarr = new Object[3];
                    objarr[0] = hop.getId();
                    objarr[1] = hop.getPriority();
                    objarr[2] = hop.getName();
                    toRemove.add(session.newInstance(ResourceRequestClusterJ.ResourceRequestDTO.class, objarr));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<ResourceRequestClusterJ.ResourceRequestDTO> toModify = new ArrayList<ResourceRequestClusterJ.ResourceRequestDTO>();
                for (HopResourceRequest hop : modified) {
                    toModify.add(createPersistable(hop, session)); 
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    private HopResourceRequest createHopResourceRequest(ResourceRequestDTO resourceRequestDTO)
        throws StorageException {
      try {
        return new HopResourceRequest(resourceRequestDTO.getappschedulinginfo_id(),
                                        resourceRequestDTO.getpriority(),
                                        resourceRequestDTO.getname(),
                                        CompressionUtils.decompress(
                                            resourceRequestDTO
                                                .getresourcerequeststate()));
      } catch (IOException e) {
        throw new StorageException(e);
      } catch (DataFormatException e) {
        throw new StorageException(e);
      }
    }

    private ResourceRequestDTO createPersistable(HopResourceRequest hop, HopsSession session) throws StorageException {
        ResourceRequestClusterJ.ResourceRequestDTO resourceRequestDTO = session.newInstance(ResourceRequestClusterJ.ResourceRequestDTO.class);
        
        resourceRequestDTO.setappschedulinginfo_id(hop.getId());
        resourceRequestDTO.setpriority(hop.getPriority());
        resourceRequestDTO.setname(hop.getName());
      try {
        resourceRequestDTO.setresourcerequeststate(
            CompressionUtils.compress(hop.getResourcerequeststate()));
      } catch (IOException e) {
        throw new StorageException(e);
      }

      return resourceRequestDTO;
    }
    
    private List<HopResourceRequest> createResourceRequestList(List<ResourceRequestClusterJ.ResourceRequestDTO> results)
        throws StorageException {
        List<HopResourceRequest> resourceRequests = new ArrayList<HopResourceRequest>();
        for (ResourceRequestClusterJ.ResourceRequestDTO persistable : results) {
            resourceRequests.add(createHopResourceRequest(persistable));
        }
        return resourceRequests;
    }
    
    private Map<String, List<HopResourceRequest>> createMap(List<ResourceRequestDTO> results)
        throws StorageException {
      Map<String, List<HopResourceRequest>> map = new HashMap<String, List<HopResourceRequest>>();
      for(ResourceRequestDTO dto: results){
        HopResourceRequest hop = createHopResourceRequest(dto);
        if(map.get(hop.getId())==null){
          map.put(hop.getId(), new ArrayList<HopResourceRequest>());
        }
        map.get(hop.getId()).add(hop);
      }
      return map;
    }
}
