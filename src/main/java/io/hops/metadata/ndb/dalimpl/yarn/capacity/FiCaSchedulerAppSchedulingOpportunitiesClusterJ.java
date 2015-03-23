

package io.hops.metadata.ndb.dalimpl.yarn.capacity;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;

import io.hops.exception.StorageException;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.dal.capacity.FiCaSchedulerAppSchedulingOpportunitiesDataAccess;
import io.hops.metadata.yarn.tabledef.capacity.FiCaSchedulerAppSchedulingOpportunitiesTableDef;
import io.hops.metadata.hdfs.entity.yarn.capacity.HopFiCaSchedulerAppSchedulingOpportunities;
import io.hops.metadata.ndb.ClusterjConnector;

public class FiCaSchedulerAppSchedulingOpportunitiesClusterJ implements
    FiCaSchedulerAppSchedulingOpportunitiesTableDef,
    FiCaSchedulerAppSchedulingOpportunitiesDataAccess<HopFiCaSchedulerAppSchedulingOpportunities> {

    
    @PersistenceCapable(table = TABLE_NAME)
    public interface FiCaSchedulerAppSchedulingOpportunitiesDTO {

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
    public HopFiCaSchedulerAppSchedulingOpportunities findById(int id) throws
        StorageException {
        HopsSession session = connector.obtainSession();

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
        HopsSession session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopFiCaSchedulerAppSchedulingOpportunities hop : removed) {
                    FiCaSchedulerAppSchedulingOpportunitiesClusterJ.FiCaSchedulerAppSchedulingOpportunitiesDTO persistable = session.newInstance(FiCaSchedulerAppSchedulingOpportunitiesClusterJ.FiCaSchedulerAppSchedulingOpportunitiesDTO.class, hop.getSchedulerapp_id());
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
       return new HopFiCaSchedulerAppSchedulingOpportunities(fiCaSchedulerAppSchedulingOpportunitiesDTO.getschedulerappid(),
                                                            fiCaSchedulerAppSchedulingOpportunitiesDTO.getpriorityid());
   }

    private FiCaSchedulerAppSchedulingOpportunitiesDTO createPersistable(HopFiCaSchedulerAppSchedulingOpportunities hop, HopsSession session) throws StorageException {
        FiCaSchedulerAppSchedulingOpportunitiesClusterJ.FiCaSchedulerAppSchedulingOpportunitiesDTO fiCaSchedulerAppSchedulingOpportunitiesDTO = session.newInstance(FiCaSchedulerAppSchedulingOpportunitiesClusterJ.FiCaSchedulerAppSchedulingOpportunitiesDTO.class);
        
        fiCaSchedulerAppSchedulingOpportunitiesDTO.setschedulerappid(hop.getSchedulerapp_id());
        fiCaSchedulerAppSchedulingOpportunitiesDTO.setpriorityid(hop.getPriority_id());
        
        return fiCaSchedulerAppSchedulingOpportunitiesDTO;
    }
    
}
