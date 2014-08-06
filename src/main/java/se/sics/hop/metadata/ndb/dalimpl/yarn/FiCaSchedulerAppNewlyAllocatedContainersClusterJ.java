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
import se.sics.hop.metadata.hdfs.entity.yarn.HopFiCaSchedulerAppNewlyAllocatedContainers;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.FiCaSchedulerAppNewlyAllocatedContainersDataAccess;
import se.sics.hop.metadata.yarn.tabledef.FiCaSchedulerAppNewlyAllocatedContainersTableDef;

/**
 *
 * @author Nikos Stanogias <niksta@sics.se>
 */
public class FiCaSchedulerAppNewlyAllocatedContainersClusterJ implements FiCaSchedulerAppNewlyAllocatedContainersTableDef, FiCaSchedulerAppNewlyAllocatedContainersDataAccess<HopFiCaSchedulerAppNewlyAllocatedContainers> {


    @PersistenceCapable(table = TABLE_NAME)
    public interface FiCaSchedulerAppNewlyAllocatedContainersDTO {

        @PrimaryKey
        @Column(name = FICASCHEDULERAPP_ID)
        String getficaschedulerappid();
        void setficaschedulerappid(String ficaschedulerappid);

        @Column(name = RMCONTAINER_ID)
        String getrmcontainerid();
        void setrmcontainerid(String rmcontainerid);    
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public HopFiCaSchedulerAppNewlyAllocatedContainers findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO fiCaSchedulerAppNewlyAllocatedContainersDTO = null;
        if (session != null) {
            fiCaSchedulerAppNewlyAllocatedContainersDTO = session.find(FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO.class, id);
        }
        if (fiCaSchedulerAppNewlyAllocatedContainersDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopFiCaSchedulerAppNewlyAllocatedContainers(fiCaSchedulerAppNewlyAllocatedContainersDTO);
    }

    @Override
    public void prepare(Collection<HopFiCaSchedulerAppNewlyAllocatedContainers> modified, Collection<HopFiCaSchedulerAppNewlyAllocatedContainers> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopFiCaSchedulerAppNewlyAllocatedContainers hop : removed) {
                    FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO persistable = session.newInstance(FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO.class, hop.getFicaschedulerapp_id());
                    session.deletePersistent(persistable);
                }
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
        return new HopFiCaSchedulerAppNewlyAllocatedContainers(fiCaSchedulerAppNewlyAllocatedContainersDTO.getficaschedulerappid(),
                                                               fiCaSchedulerAppNewlyAllocatedContainersDTO.getrmcontainerid());
    }

    private FiCaSchedulerAppNewlyAllocatedContainersDTO createPersistable(HopFiCaSchedulerAppNewlyAllocatedContainers hop, Session session) {
        FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO fiCaSchedulerAppNewlyAllocatedContainersDTO = session.newInstance(FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO.class);
        
        fiCaSchedulerAppNewlyAllocatedContainersDTO.setficaschedulerappid(hop.getFicaschedulerapp_id());
        fiCaSchedulerAppNewlyAllocatedContainersDTO.setrmcontainerid(hop.getRmcontainer_id());
        
        return fiCaSchedulerAppNewlyAllocatedContainersDTO;
    }   
}
