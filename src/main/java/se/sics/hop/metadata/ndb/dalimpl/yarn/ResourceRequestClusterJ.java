/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
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
        String getappschedulinginfoid();
        void setappschedulinginfoid(String appschedulinginfoid);

        @Column(name = PRIORITY)
        int getpriority();
        void setpriority(int priority);

        @Column(name = RESOURCEREQUESTSTATE)
        byte[] getresourcerequeststate();
        void setresourcerequeststate(byte[] resourcerequeststate);
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public HopResourceRequest findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        ResourceRequestClusterJ.ResourceRequestDTO resourceRequestDTO = null;
        if (session != null) {
            resourceRequestDTO = session.find(ResourceRequestClusterJ.ResourceRequestDTO.class, id);
        }
        if (resourceRequestDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }
        return createHopResourceRequest(resourceRequestDTO);
    }

    @Override
    public void prepare(Collection<HopResourceRequest> modified, Collection<HopResourceRequest> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                List<ResourceRequestClusterJ.ResourceRequestDTO> toRemove = new ArrayList<ResourceRequestClusterJ.ResourceRequestDTO>();
                for (HopResourceRequest hop : removed) {
                    toRemove.add(session.newInstance(ResourceRequestClusterJ.ResourceRequestDTO.class, hop.getId())); 
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
        return new HopResourceRequest(resourceRequestDTO.getappschedulinginfoid(),
                                        resourceRequestDTO.getpriority(),
                                        resourceRequestDTO.getresourcerequeststate());
    }

    private ResourceRequestDTO createPersistable(HopResourceRequest hop, Session session) {
        ResourceRequestClusterJ.ResourceRequestDTO resourceRequestDTO = session.newInstance(ResourceRequestClusterJ.ResourceRequestDTO.class);
        
        resourceRequestDTO.setappschedulinginfoid(hop.getId());
        resourceRequestDTO.setpriority(hop.getPriority());
        resourceRequestDTO.setresourcerequeststate(hop.getResourcerequeststate());
        
        return resourceRequestDTO;
    }  
}
