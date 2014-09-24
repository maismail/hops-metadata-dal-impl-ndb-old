/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
        @Column(name = SCHEDULERAPP_ID)
        String getschedulerapp_id();
        void setschedulerapp_id(String schedulerapp_id);

        @Column(name = RMCONTAINER_ID)
        String getrmcontainerid();
        void setrmcontainerid(String rmcontainerid);    
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public List<HopFiCaSchedulerAppNewlyAllocatedContainers> findById(String ficaId) throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();

            QueryDomainType<FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO> dobj = qb.createQueryDefinition(FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO.class);
            Predicate pred1 = dobj.get("schedulerapp_id").equal(dobj.param("schedulerapp_id"));
            dobj.where(pred1);
            Query<FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO> query = session.createQuery(dobj);
            query.setParameter("schedulerapp_id", ficaId);

            List<FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO> results = query.getResultList();
            return createFiCaSchedulerAppNewlyAllocatedContainersList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void prepare(Collection<HopFiCaSchedulerAppNewlyAllocatedContainers> modified, Collection<HopFiCaSchedulerAppNewlyAllocatedContainers> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                List<FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO> toRemove = new ArrayList<FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO>();
                for (HopFiCaSchedulerAppNewlyAllocatedContainers hop : removed) {
                    Object[] objarr = new Object[2];
                    objarr[0] = hop.getSchedulerapp_id();
                    objarr[1] = hop.getRmcontainer_id();
                    toRemove.add(session.newInstance(FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO.class, objarr));
                }
                session.deletePersistentAll(toRemove);
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
        return new HopFiCaSchedulerAppNewlyAllocatedContainers(fiCaSchedulerAppNewlyAllocatedContainersDTO.getschedulerapp_id(),
                                                               fiCaSchedulerAppNewlyAllocatedContainersDTO.getrmcontainerid());
    }

    private FiCaSchedulerAppNewlyAllocatedContainersDTO createPersistable(HopFiCaSchedulerAppNewlyAllocatedContainers hop, Session session) {
        FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO fiCaSchedulerAppNewlyAllocatedContainersDTO = session.newInstance(FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO.class);
        
        fiCaSchedulerAppNewlyAllocatedContainersDTO.setschedulerapp_id(hop.getSchedulerapp_id());
        fiCaSchedulerAppNewlyAllocatedContainersDTO.setrmcontainerid(hop.getRmcontainer_id());
        
        return fiCaSchedulerAppNewlyAllocatedContainersDTO;
    }
    
    private List<HopFiCaSchedulerAppNewlyAllocatedContainers> createFiCaSchedulerAppNewlyAllocatedContainersList(List<FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO> results) {
        List<HopFiCaSchedulerAppNewlyAllocatedContainers> ficaSchedulerAppNewlyAllocatedContainers = new ArrayList<HopFiCaSchedulerAppNewlyAllocatedContainers>();
        for (FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO persistable : results) {
            ficaSchedulerAppNewlyAllocatedContainers.add(createHopFiCaSchedulerAppNewlyAllocatedContainers(persistable));
        }
        return ficaSchedulerAppNewlyAllocatedContainers;
    }
}
