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
import se.sics.hop.metadata.hdfs.entity.yarn.HopFiCaSchedulerAppReservedContainers;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.FiCaSchedulerAppReservedContainersDataAccess;
import se.sics.hop.metadata.yarn.tabledef.FiCaSchedulerAppReservedContainersTableDef;
import static se.sics.hop.metadata.yarn.tabledef.FiCaSchedulerAppReservedContainersTableDef.PRIORITY_ID;

/**
 *
 * @author nickstanogias
 */
public class FiCaSchedulerAppReservedContainersClusterJ implements FiCaSchedulerAppReservedContainersTableDef, FiCaSchedulerAppReservedContainersDataAccess<HopFiCaSchedulerAppReservedContainers>{

    @PersistenceCapable(table = TABLE_NAME)
    public interface FiCaSchedulerAppReservedContainersDTO {

        @PrimaryKey
        @Column(name = FICASCHEDULERAPP_ID)
        int getficaschedulerappid();
        void setficaschedulerappid(int ficaschedulerappid);

        @Column(name = PRIORITY_ID)
        int getpriorityid();
        void setpriorityid(int priorityid);
        
        @Column(name = NODEID)
        int getnodeid();
        void setnodeid(int nodeid);
        
        @Column(name = RMCONTAINER)
        int getrmcontainer();
        void setrmcontainer(int rmcontainer);
    }
    
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
        
    @Override
    public HopFiCaSchedulerAppReservedContainers findById(int id) throws StorageException {
        Session session = connector.obtainSession();
        
        FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO fiCaSchedulerAppReservedContainersDTO = null;
        if(session !=  null) {
            fiCaSchedulerAppReservedContainersDTO = session.find(FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO.class, id);
        }
        if(fiCaSchedulerAppReservedContainersDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }
        
        return createHopFiCaSchedulerAppReservedContainers(fiCaSchedulerAppReservedContainersDTO);
    }

    @Override
    public void prepare(Collection<HopFiCaSchedulerAppReservedContainers> modified, Collection<HopFiCaSchedulerAppReservedContainers> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopFiCaSchedulerAppReservedContainers hop : removed) {
                    FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO persistable = session.newInstance(FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO.class, hop.getFicaschedulerapp_id());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopFiCaSchedulerAppReservedContainers hop : modified) {
                    FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    private HopFiCaSchedulerAppReservedContainers createHopFiCaSchedulerAppReservedContainers(FiCaSchedulerAppReservedContainersDTO fiCaSchedulerAppReservedContainersDTO) {
        return new HopFiCaSchedulerAppReservedContainers(fiCaSchedulerAppReservedContainersDTO.getficaschedulerappid(),
                                                         fiCaSchedulerAppReservedContainersDTO.getpriorityid(),
                                                         fiCaSchedulerAppReservedContainersDTO.getnodeid(),
                                                         fiCaSchedulerAppReservedContainersDTO.getrmcontainer());
    }

    private FiCaSchedulerAppReservedContainersDTO createPersistable(HopFiCaSchedulerAppReservedContainers hop, Session session) {
        FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO fiCaSchedulerAppReservedContainersDTO = session.newInstance(FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO.class);
        
        fiCaSchedulerAppReservedContainersDTO.setficaschedulerappid(hop.getFicaschedulerapp_id());
        fiCaSchedulerAppReservedContainersDTO.setpriorityid(hop.getPriority_id());
        fiCaSchedulerAppReservedContainersDTO.setnodeid(hop.getNodeid());
        fiCaSchedulerAppReservedContainersDTO.setrmcontainer(hop.getRmcontainer());
        
        return fiCaSchedulerAppReservedContainersDTO;
    }
    
}
