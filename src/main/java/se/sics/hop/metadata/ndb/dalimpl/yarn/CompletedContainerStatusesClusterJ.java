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
import se.sics.hop.metadata.hdfs.entity.yarn.HopCompletedContainersStatuses;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.CompletedContainersStatusesDataAccess;
import se.sics.hop.metadata.yarn.tabledef.CompletedContainerStatusesTableDef;

/**
 *
 * @author nickstanogias
 */
public class CompletedContainerStatusesClusterJ implements CompletedContainerStatusesTableDef, CompletedContainersStatusesDataAccess<HopCompletedContainersStatuses>{


    @PersistenceCapable(table = TABLE_NAME)
    public interface CompletedContainerStatusesDTO {

        @PrimaryKey
        @Column(name = ALLOCATERESPONSE_ID)
        int getallocateresponseid();
        void setallocateresponseid(int allocateresponseid);

        @Column(name = CONTAINERSTATUS_ID)
        int getcontainerstatusid();
        void setcontainerstatusid(int containerstatusid);
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public HopCompletedContainersStatuses findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        CompletedContainerStatusesClusterJ.CompletedContainerStatusesDTO completedContainerStatusesDTO = null;
        if (session != null) {
            completedContainerStatusesDTO = session.find(CompletedContainerStatusesClusterJ.CompletedContainerStatusesDTO.class, id);
        }
        if (completedContainerStatusesDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopCompletedContainersStatuses(completedContainerStatusesDTO);
    }
    
    @Override
    public void prepare(Collection<HopCompletedContainersStatuses> modified, Collection<HopCompletedContainersStatuses> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopCompletedContainersStatuses hop : removed) {
                    CompletedContainerStatusesClusterJ.CompletedContainerStatusesDTO persistable = session.newInstance(CompletedContainerStatusesClusterJ.CompletedContainerStatusesDTO.class, hop.getAllocateresponse_id());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopCompletedContainersStatuses hop : modified) {
                    CompletedContainerStatusesClusterJ.CompletedContainerStatusesDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    private HopCompletedContainersStatuses createHopCompletedContainersStatuses(CompletedContainerStatusesDTO completedContainerStatusesDTO) {
        return new HopCompletedContainersStatuses(completedContainerStatusesDTO.getallocateresponseid(),
                                                    completedContainerStatusesDTO.getcontainerstatusid());
    }

    private CompletedContainerStatusesDTO createPersistable(HopCompletedContainersStatuses hop, Session session) {
        CompletedContainerStatusesClusterJ.CompletedContainerStatusesDTO completedContainerStatusesDTO = session.newInstance(CompletedContainerStatusesClusterJ.CompletedContainerStatusesDTO.class);
        
        completedContainerStatusesDTO.setallocateresponseid(hop.getAllocateresponse_id());
        completedContainerStatusesDTO.setcontainerstatusid(hop.getContainerstatus_id());
        
        return completedContainerStatusesDTO;
    }
    
    
}
