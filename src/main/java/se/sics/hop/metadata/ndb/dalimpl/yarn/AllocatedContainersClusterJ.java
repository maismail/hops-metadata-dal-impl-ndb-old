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
import java.util.Collection;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopAllocatedContainers;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.AllocatedContainersDataAccess;
import se.sics.hop.metadata.yarn.tabledef.AllocatedContainersTableDef;

/**
 *
 * @author nickstanogias
 */
public class AllocatedContainersClusterJ implements AllocatedContainersTableDef, AllocatedContainersDataAccess<HopAllocatedContainers>{

    @PersistenceCapable(table = TABLE_NAME)
    public interface AllocatedContainersDTO {

        @PrimaryKey
        @Column(name = ALLOCATERESPONSE_ID)
        int getallocateresponseid();
        void setallocateresponseid(int allocateresponseid);

        @Column(name = CONTAINER_ID)
        int getcontainerid();
        void setcontainerid(int containerid);
        
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public HopAllocatedContainers findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        AllocatedContainersClusterJ.AllocatedContainersDTO allocatedContainersDTO = null;
        if (session != null) {
            allocatedContainersDTO = session.find(AllocatedContainersClusterJ.AllocatedContainersDTO.class, id);
        }
        if (allocatedContainersDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopAllocatedContainers(allocatedContainersDTO);
    }

    @Override
    public void prepare(Collection<HopAllocatedContainers> modified, Collection<HopAllocatedContainers> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopAllocatedContainers hop : removed) {
                    AllocatedContainersClusterJ.AllocatedContainersDTO persistable = session.newInstance(AllocatedContainersClusterJ.AllocatedContainersDTO.class, hop.getAllocateresponse_id());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopAllocatedContainers hop : modified) {
                    AllocatedContainersClusterJ.AllocatedContainersDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    private HopAllocatedContainers createHopAllocatedContainers(AllocatedContainersDTO allocatedContainersDTO) {
        return new HopAllocatedContainers(allocatedContainersDTO.getallocateresponseid(),
                                            allocatedContainersDTO.getcontainerid());
    }

    private AllocatedContainersDTO createPersistable(HopAllocatedContainers hop, Session session) {
        AllocatedContainersClusterJ.AllocatedContainersDTO allocatedContainersDTO = session.newInstance(AllocatedContainersClusterJ.AllocatedContainersDTO.class);
        
        allocatedContainersDTO.setallocateresponseid(hop.getAllocateresponse_id());
        allocatedContainersDTO.setcontainerid(hop.getContainer_id());
        
        return allocatedContainersDTO;
    }
    
}
