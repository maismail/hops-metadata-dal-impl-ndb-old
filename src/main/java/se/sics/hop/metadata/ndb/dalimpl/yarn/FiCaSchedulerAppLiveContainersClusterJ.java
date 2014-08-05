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
import se.sics.hop.metadata.hdfs.entity.yarn.HopFiCaSchedulerAppLiveContainers;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.FiCaSchedulerAppLiveContainersDataAccess;
import se.sics.hop.metadata.yarn.tabledef.FiCaSchedulerAppLiveContainersTableDef;

/**
 *
 * @author Nikos Stanogias <niksta@sics.se>
 */
public class FiCaSchedulerAppLiveContainersClusterJ implements FiCaSchedulerAppLiveContainersTableDef, FiCaSchedulerAppLiveContainersDataAccess<HopFiCaSchedulerAppLiveContainers>{

    @PersistenceCapable(table = TABLE_NAME)
    public interface FiCaSchedulerAppLiveContainersDTO {

        @PrimaryKey
        @Column(name = FICASCHEDULERAPP_ID)
        String getficaschedulerappid();
        void setficaschedulerappid(String ficaschedulerappid);

        @Column(name = CONTAINERID_ID)
        String getcontaineridid();
        void setcontaineridid(String containeridid);
        
        @Column(name = RMCONTAINER_ID)
        String getrmcontainerid();
        void setrmcontainerid(String rmcontainerid);
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public HopFiCaSchedulerAppLiveContainers findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO fiCaSchedulerAppLiveContainersDTO = null;
        if (session != null) {
            fiCaSchedulerAppLiveContainersDTO = session.find(FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO.class, id);
        }
        if (fiCaSchedulerAppLiveContainersDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopFiCaSchedulerAppLiveContainers(fiCaSchedulerAppLiveContainersDTO);
    }

    @Override
    public void prepare(Collection<HopFiCaSchedulerAppLiveContainers> modified, Collection<HopFiCaSchedulerAppLiveContainers> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopFiCaSchedulerAppLiveContainers hop : removed) {
                    FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO persistable = session.newInstance(FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO.class, hop.getFicaschedulerapp_id());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopFiCaSchedulerAppLiveContainers hop : modified) {
                    FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    private HopFiCaSchedulerAppLiveContainers createHopFiCaSchedulerAppLiveContainers(FiCaSchedulerAppLiveContainersDTO fiCaSchedulerAppLiveContainersDTO) {
        return new HopFiCaSchedulerAppLiveContainers(fiCaSchedulerAppLiveContainersDTO.getficaschedulerappid(),
                                                    fiCaSchedulerAppLiveContainersDTO.getcontaineridid(),
                                                    fiCaSchedulerAppLiveContainersDTO.getrmcontainerid());
    }

    private FiCaSchedulerAppLiveContainersDTO createPersistable(HopFiCaSchedulerAppLiveContainers hop, Session session) {
        FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO fiCaSchedulerAppLiveContainersDTO = session.newInstance(FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO.class);
        
        fiCaSchedulerAppLiveContainersDTO.setficaschedulerappid(hop.getFicaschedulerapp_id());
        fiCaSchedulerAppLiveContainersDTO.setcontaineridid(hop.getContainerid_id());
        fiCaSchedulerAppLiveContainersDTO.setrmcontainerid(hop.getRmcontainer_id());
        
        return fiCaSchedulerAppLiveContainersDTO;
    }
    
}
