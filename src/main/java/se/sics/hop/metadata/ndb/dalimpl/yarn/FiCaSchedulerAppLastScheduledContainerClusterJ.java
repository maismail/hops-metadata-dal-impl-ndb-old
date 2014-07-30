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
import se.sics.hop.metadata.hdfs.entity.yarn.HopFiCaSchedulerAppLastScheduledContainer;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.FiCaSchedulerAppLastScheduledContainerDataAccess;
import se.sics.hop.metadata.yarn.tabledef.FiCaSchedulerAppLastScheduledContainerTableDef;

/**
 *
 * @author Nikos Stanogias <niksta@sics.se>
 */
public class FiCaSchedulerAppLastScheduledContainerClusterJ implements FiCaSchedulerAppLastScheduledContainerTableDef, FiCaSchedulerAppLastScheduledContainerDataAccess<HopFiCaSchedulerAppLastScheduledContainer>{


    @PersistenceCapable(table = TABLE_NAME)
    public interface FiCaSchedulerAppLastScheduledContainerDTO {

        @PrimaryKey
        @Column(name = FICASCHEDULERAPP_ID)
        int getficaschedulerappid();
        void setficaschedulerappid(int ficaschedulerappid);

        @Column(name = PRIORITY_ID)
        int getpriorityid();
        void setpriorityid(int priorityid);
        
        @Column(name = TIME)
        long gettime();
        void settime(long time);
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public HopFiCaSchedulerAppLastScheduledContainer findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        FiCaSchedulerAppLastScheduledContainerClusterJ.FiCaSchedulerAppLastScheduledContainerDTO fiCaSchedulerAppLastScheduledContainerDTO = null;
        if (session != null) {
            fiCaSchedulerAppLastScheduledContainerDTO = session.find(FiCaSchedulerAppLastScheduledContainerClusterJ.FiCaSchedulerAppLastScheduledContainerDTO.class, id);
        }
        if (fiCaSchedulerAppLastScheduledContainerDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopFiCaSchedulerAppLastScheduledContainer(fiCaSchedulerAppLastScheduledContainerDTO);
    }

    @Override
    public void prepare(Collection<HopFiCaSchedulerAppLastScheduledContainer> modified, Collection<HopFiCaSchedulerAppLastScheduledContainer> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopFiCaSchedulerAppLastScheduledContainer hop : removed) {
                    FiCaSchedulerAppLastScheduledContainerClusterJ.FiCaSchedulerAppLastScheduledContainerDTO persistable = session.newInstance(FiCaSchedulerAppLastScheduledContainerClusterJ.FiCaSchedulerAppLastScheduledContainerDTO.class, hop.getFicaschedulerapp_id());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopFiCaSchedulerAppLastScheduledContainer hop : modified) {
                    FiCaSchedulerAppLastScheduledContainerClusterJ.FiCaSchedulerAppLastScheduledContainerDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    private HopFiCaSchedulerAppLastScheduledContainer createHopFiCaSchedulerAppLastScheduledContainer(FiCaSchedulerAppLastScheduledContainerDTO fiCaSchedulerAppLastScheduledContainerDTO) {
        return new HopFiCaSchedulerAppLastScheduledContainer(fiCaSchedulerAppLastScheduledContainerDTO.getficaschedulerappid(),
                                                              fiCaSchedulerAppLastScheduledContainerDTO.getpriorityid(),
                                                                fiCaSchedulerAppLastScheduledContainerDTO.gettime());
    }

    private FiCaSchedulerAppLastScheduledContainerDTO createPersistable(HopFiCaSchedulerAppLastScheduledContainer hop, Session session) {
        FiCaSchedulerAppLastScheduledContainerClusterJ.FiCaSchedulerAppLastScheduledContainerDTO fiCaSchedulerAppLastScheduledContainerDTO = session.newInstance(FiCaSchedulerAppLastScheduledContainerClusterJ.FiCaSchedulerAppLastScheduledContainerDTO.class);
        
        fiCaSchedulerAppLastScheduledContainerDTO.setficaschedulerappid(hop.getFicaschedulerapp_id());
        fiCaSchedulerAppLastScheduledContainerDTO.setpriorityid(hop.getPriority_id());
        fiCaSchedulerAppLastScheduledContainerDTO.settime(hop.getTime());
        
        return fiCaSchedulerAppLastScheduledContainerDTO;
    }
    
}
