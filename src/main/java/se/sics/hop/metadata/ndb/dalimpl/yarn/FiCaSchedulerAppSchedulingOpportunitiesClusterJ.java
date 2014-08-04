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
import se.sics.hop.metadata.hdfs.entity.yarn.HopFiCaSchedulerAppSchedulingOpportunities;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.FiCaSchedulerAppSchedulingOpportunitiesDataAccess;
import se.sics.hop.metadata.yarn.tabledef.FiCaSchedulerAppSchedulingOpportunitiesTableDef;

/**
 *
 * @author nickstanogias
 */
public class FiCaSchedulerAppSchedulingOpportunitiesClusterJ implements FiCaSchedulerAppSchedulingOpportunitiesTableDef, FiCaSchedulerAppSchedulingOpportunitiesDataAccess<HopFiCaSchedulerAppSchedulingOpportunities>{

    
    @PersistenceCapable(table = TABLE_NAME)
    public interface FiCaSchedulerAppSchedulingOpportunitiesDTO {

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
    public HopFiCaSchedulerAppSchedulingOpportunities findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        FiCaSchedulerAppSchedulingOpportunitiesClusterJ.FiCaSchedulerAppSchedulingOpportunitiesDTO fiCaSchedulerAppSchedulingOpportunitiesDTO = null;
        if (session != null) {
            fiCaSchedulerAppSchedulingOpportunitiesDTO = session.find(FiCaSchedulerAppSchedulingOpportunitiesClusterJ.FiCaSchedulerAppSchedulingOpportunitiesDTO.class, id);
        }
        if (fiCaSchedulerAppSchedulingOpportunitiesDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopFiCaSchedulerAppSchedulingOpportunities(fiCaSchedulerAppSchedulingOpportunitiesDTO);
    }

    @Override
    public void prepare(Collection<HopFiCaSchedulerAppSchedulingOpportunities> modified, Collection<HopFiCaSchedulerAppSchedulingOpportunities> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopFiCaSchedulerAppSchedulingOpportunities hop : removed) {
                    FiCaSchedulerAppSchedulingOpportunitiesClusterJ.FiCaSchedulerAppSchedulingOpportunitiesDTO persistable = session.newInstance(FiCaSchedulerAppSchedulingOpportunitiesClusterJ.FiCaSchedulerAppSchedulingOpportunitiesDTO.class, hop.getFicaschedulerapp_id());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopFiCaSchedulerAppSchedulingOpportunities hop : modified) {
                    FiCaSchedulerAppSchedulingOpportunitiesClusterJ.FiCaSchedulerAppSchedulingOpportunitiesDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
   private HopFiCaSchedulerAppSchedulingOpportunities createHopFiCaSchedulerAppSchedulingOpportunities(FiCaSchedulerAppSchedulingOpportunitiesDTO fiCaSchedulerAppSchedulingOpportunitiesDTO) {
       return new HopFiCaSchedulerAppSchedulingOpportunities(fiCaSchedulerAppSchedulingOpportunitiesDTO.getficaschedulerappid(),
                                                            fiCaSchedulerAppSchedulingOpportunitiesDTO.getpriorityid());
   }

    private FiCaSchedulerAppSchedulingOpportunitiesDTO createPersistable(HopFiCaSchedulerAppSchedulingOpportunities hop, Session session) {
        FiCaSchedulerAppSchedulingOpportunitiesClusterJ.FiCaSchedulerAppSchedulingOpportunitiesDTO fiCaSchedulerAppSchedulingOpportunitiesDTO = session.newInstance(FiCaSchedulerAppSchedulingOpportunitiesClusterJ.FiCaSchedulerAppSchedulingOpportunitiesDTO.class);
        
        fiCaSchedulerAppSchedulingOpportunitiesDTO.setficaschedulerappid(hop.getFicaschedulerapp_id());
        fiCaSchedulerAppSchedulingOpportunitiesDTO.setpriorityid(hop.getPriority_id());
        
        return fiCaSchedulerAppSchedulingOpportunitiesDTO;
    }
    
}
