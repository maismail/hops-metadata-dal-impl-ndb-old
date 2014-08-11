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
import se.sics.hop.metadata.hdfs.entity.yarn.HopFiCaSchedulerAppReservations;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.FiCaSchedulerAppReservationsDataAccess;
import se.sics.hop.metadata.yarn.tabledef.FiCaSchedulerAppReservationsTableDef;

/**
 *
 * @author nickstanogias
 */
public class FiCaSchedulerAppReservationsClusterJ implements FiCaSchedulerAppReservationsTableDef, FiCaSchedulerAppReservationsDataAccess<HopFiCaSchedulerAppReservations>{

    @PersistenceCapable(table = TABLE_NAME)
    public interface FiCaSchedulerAppReservationsDTO {

        @PrimaryKey
        @Column(name = FICASCHEDULERAPP_ID)
        String getficaschedulerappid();
        void setficaschedulerappid(String ficaschedulerappid);

        @Column(name = PRIORITY_ID)
        int getpriorityid();
        void setpriorityid(int priorityid);
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public HopFiCaSchedulerAppReservations findById(int id) throws StorageException {
        Session session = connector.obtainSession();

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
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopFiCaSchedulerAppReservations hop : removed) {
                    FiCaSchedulerAppReservationsClusterJ.FiCaSchedulerAppReservationsDTO persistable = session.newInstance(FiCaSchedulerAppReservationsClusterJ.FiCaSchedulerAppReservationsDTO.class, hop.getFicaschedulerapp_id());
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
        return new HopFiCaSchedulerAppReservations(fiCaSchedulerAppReservationsDTO.getficaschedulerappid(),
                                                    fiCaSchedulerAppReservationsDTO.getpriorityid());
    }

    private FiCaSchedulerAppReservationsDTO createPersistable(HopFiCaSchedulerAppReservations hop, Session session) {
        FiCaSchedulerAppReservationsClusterJ.FiCaSchedulerAppReservationsDTO fiCaSchedulerAppReservationsDTO = session.newInstance(FiCaSchedulerAppReservationsClusterJ.FiCaSchedulerAppReservationsDTO.class);
        
        fiCaSchedulerAppReservationsDTO.setficaschedulerappid(hop.getFicaschedulerapp_id());
        fiCaSchedulerAppReservationsDTO.setpriorityid(hop.getPriority_id());
        
        return fiCaSchedulerAppReservationsDTO;
    }
}
