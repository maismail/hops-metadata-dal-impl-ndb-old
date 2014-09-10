/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopResourceRequest;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.ResourceRequestDataAccess;
import se.sics.hop.metadata.yarn.tabledef.ResourceRequestTableDef;

/**
 *
 * @author Nikos Stanogias <niksta@sics.se>
 */
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
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();

            QueryDomainType<ResourceRequestClusterJ.ResourceRequestDTO> dobj = qb.createQueryDefinition(ResourceRequestClusterJ.ResourceRequestDTO.class);
            Predicate pred1 = dobj.get("appschedulinginfo_id").equal(dobj.param("appschedulinginfo_id"));
            dobj.where(pred1);
            Query<ResourceRequestClusterJ.ResourceRequestDTO> query = session.createQuery(dobj);
            query.setParameter("appschedulinginfo_id", id);

            List<ResourceRequestClusterJ.ResourceRequestDTO> results = query.getResultList();
            return createResourceRequestList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void prepare(Collection<HopResourceRequest> modified, Collection<HopResourceRequest> removed) throws StorageException {
        Session session = connector.obtainSession();
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
    
    private HopResourceRequest createHopResourceRequest(ResourceRequestDTO resourceRequestDTO) {
        return new HopResourceRequest(resourceRequestDTO.getappschedulinginfo_id(),
                                        resourceRequestDTO.getpriority(),
                                        resourceRequestDTO.getname(),
                                        resourceRequestDTO.getresourcerequeststate());
    }

    private ResourceRequestDTO createPersistable(HopResourceRequest hop, Session session) {
        ResourceRequestClusterJ.ResourceRequestDTO resourceRequestDTO = session.newInstance(ResourceRequestClusterJ.ResourceRequestDTO.class);
        
        resourceRequestDTO.setappschedulinginfo_id(hop.getId());
        resourceRequestDTO.setpriority(hop.getPriority());
        resourceRequestDTO.setname(hop.getName());
        resourceRequestDTO.setresourcerequeststate(hop.getResourcerequeststate());
        
        return resourceRequestDTO;
    }
    
    private List<HopResourceRequest> createResourceRequestList(List<ResourceRequestClusterJ.ResourceRequestDTO> results) {
        List<HopResourceRequest> resourceRequests = new ArrayList<HopResourceRequest>();
        for (ResourceRequestClusterJ.ResourceRequestDTO persistable : results) {
            resourceRequests.add(createHopResourceRequest(persistable));
        }
        return resourceRequests;
    }
}
