/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package se.sics.hop.metadata.ndb.dalimpl.yarn.capacity;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.capacity.HopFiCaSchedulerAppReservations;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.capacity.FiCaSchedulerAppReservationsDataAccess;
import se.sics.hop.metadata.yarn.tabledef.capacity.FiCaSchedulerAppReservationsTableDef;

public class FiCaSchedulerAppReservationsClusterJ implements FiCaSchedulerAppReservationsTableDef, FiCaSchedulerAppReservationsDataAccess<HopFiCaSchedulerAppReservations>{

    @PersistenceCapable(table = TABLE_NAME)
    public interface FiCaSchedulerAppReservationsDTO {

        @PrimaryKey
        @Column(name = SCHEDULERAPP_ID)
        String getschedulerappid();
        void setschedulerappid(String schedulerappid);

        @Column(name = PRIORITY_ID)
        int getpriorityid();
        void setpriorityid(int priorityid);
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public HopFiCaSchedulerAppReservations findById(int id) throws StorageException {
        HopsSession session = connector.obtainSession();

        FiCaSchedulerAppReservationsClusterJ.FiCaSchedulerAppReservationsDTO fiCaSchedulerAppReservationsDTO = null;
        if (session != null) {
            fiCaSchedulerAppReservationsDTO = session.find(FiCaSchedulerAppReservationsClusterJ.FiCaSchedulerAppReservationsDTO.class, id);
        }
        if (fiCaSchedulerAppReservationsDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopFiCaSchedulerAppReservations(fiCaSchedulerAppReservationsDTO);
    }

    @Override
    public void prepare(Collection<HopFiCaSchedulerAppReservations> modified, Collection<HopFiCaSchedulerAppReservations> removed) throws StorageException {
        HopsSession session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopFiCaSchedulerAppReservations hop : removed) {
                    FiCaSchedulerAppReservationsClusterJ.FiCaSchedulerAppReservationsDTO persistable = session.newInstance(FiCaSchedulerAppReservationsClusterJ.FiCaSchedulerAppReservationsDTO.class, hop.getSchedulerapp_id());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopFiCaSchedulerAppReservations hop : modified) {
                    FiCaSchedulerAppReservationsClusterJ.FiCaSchedulerAppReservationsDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    
    private HopFiCaSchedulerAppReservations createHopFiCaSchedulerAppReservations(FiCaSchedulerAppReservationsDTO fiCaSchedulerAppReservationsDTO) {
        return new HopFiCaSchedulerAppReservations(fiCaSchedulerAppReservationsDTO.getschedulerappid(),
                                                    fiCaSchedulerAppReservationsDTO.getpriorityid());
    }

    private FiCaSchedulerAppReservationsDTO createPersistable(HopFiCaSchedulerAppReservations hop, HopsSession session) throws StorageException {
        FiCaSchedulerAppReservationsClusterJ.FiCaSchedulerAppReservationsDTO fiCaSchedulerAppReservationsDTO = session.newInstance(FiCaSchedulerAppReservationsClusterJ.FiCaSchedulerAppReservationsDTO.class);
        
        fiCaSchedulerAppReservationsDTO.setschedulerappid(hop.getSchedulerapp_id());
        fiCaSchedulerAppReservationsDTO.setpriorityid(hop.getPriority_id());
        
        return fiCaSchedulerAppReservationsDTO;
    }
}
