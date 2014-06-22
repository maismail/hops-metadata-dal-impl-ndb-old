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
import se.sics.hop.metadata.hdfs.entity.yarn.HopFifoSchedulerApps;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.FifoSchedulerAppsDataAccess;
import se.sics.hop.metadata.yarn.tabledef.FifoSchedulerAppsTableDef;

/**
 *
 * @author nickstanogias
 */
public class FifoSchedulerAppsClusterJ implements FifoSchedulerAppsTableDef, FifoSchedulerAppsDataAccess<HopFifoSchedulerApps>{


    @PersistenceCapable(table = TABLE_NAME)
    public interface FifoSchedulerAppsDTO {

        @PrimaryKey
        @Column(name = FIFOSCHEDULER_ID)
        int getfifoschedulerid();
        void setfifoschedulerid(int fifoscheduler_id);

        @Column(name = APPLICATIONATTEMPTID_ID)
        int getapplicationattemptid();
        void setapplicationattemptid(int applicationattempt_id);
        
        @Column(name = FICASCHEDULERAPP_ID)
        int getficaschedulerappid();
        void setficaschedulerappid(int ficaschedulerapp_id);
    }
    
    private final ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopFifoSchedulerApps findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        FifoSchedulerAppsClusterJ.FifoSchedulerAppsDTO fifoSchedulerAppsDTO = null;
        if (session != null) {
            fifoSchedulerAppsDTO = session.find(FifoSchedulerAppsClusterJ.FifoSchedulerAppsDTO.class, id);
        }
        if (fifoSchedulerAppsDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopFifoSchedulerApps(fifoSchedulerAppsDTO);
    }

    @Override
    public void prepare(Collection<HopFifoSchedulerApps> modified, Collection<HopFifoSchedulerApps> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopFifoSchedulerApps hop : removed) {
                    Object[] objarr = new Object[2];
                    objarr[0] = hop.getFifoscheduler_id();
                    objarr[1] = hop.getApplicationattemptid_id();
                    FifoSchedulerAppsClusterJ.FifoSchedulerAppsDTO persistable = session.newInstance(FifoSchedulerAppsClusterJ.FifoSchedulerAppsDTO.class, objarr);
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopFifoSchedulerApps hop : modified) {
                    FifoSchedulerAppsClusterJ.FifoSchedulerAppsDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    @Override
    public HopFifoSchedulerApps findEntry(int fifoSchedulerId, int applicationattemptidId) throws StorageException {
        Session session = connector.obtainSession();
        Object[] objarr = new Object[2];
        objarr[0] = fifoSchedulerId;
        objarr[1] = applicationattemptidId;
        FifoSchedulerAppsDTO entry = null;
        if (session != null) {
            entry = session.find(FifoSchedulerAppsDTO.class, objarr);
        }
        if (entry == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createFifoSchedulerApps(entry);
    }

    @Override
    public void createFifoSchedulerAppsEntry(HopFifoSchedulerApps entry) throws StorageException {
        Session session = connector.obtainSession();
        session.savePersistent(createPersistable(entry, session));
    }

    @Override
    public List<HopFifoSchedulerApps> getAllByFifoSchedulerId(int fifoSchedulerId) throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();

            QueryDomainType<FifoSchedulerAppsClusterJ.FifoSchedulerAppsDTO> dobj = qb.createQueryDefinition(FifoSchedulerAppsClusterJ.FifoSchedulerAppsDTO.class);
            Predicate pred1 = dobj.get("fifoschedulerid").equal(dobj.param("fifoschedulerid"));
            dobj.where(pred1);
            Query<FifoSchedulerAppsClusterJ.FifoSchedulerAppsDTO> query = session.createQuery(dobj);
            query.setParameter("fifoschedulerid", fifoSchedulerId);

            List<FifoSchedulerAppsClusterJ.FifoSchedulerAppsDTO> results = query.getResultList();
            return createFifoSchedulerAppsList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }   
    }
    
    private HopFifoSchedulerApps createFifoSchedulerApps(FifoSchedulerAppsClusterJ.FifoSchedulerAppsDTO entry) {
        HopFifoSchedulerApps hop = new HopFifoSchedulerApps(entry.getfifoschedulerid(), entry.getapplicationattemptid(), entry.getficaschedulerappid());
        return hop;
    }
    
    private HopFifoSchedulerApps createHopFifoSchedulerApps(FifoSchedulerAppsDTO fifoSchedulerAppsDTO) {
        return new HopFifoSchedulerApps(fifoSchedulerAppsDTO.getfifoschedulerid(),
                                        fifoSchedulerAppsDTO.getapplicationattemptid(),
                                        fifoSchedulerAppsDTO.getficaschedulerappid());
    }

    private FifoSchedulerAppsClusterJ.FifoSchedulerAppsDTO createPersistable(HopFifoSchedulerApps hop, Session session) {
        FifoSchedulerAppsClusterJ.FifoSchedulerAppsDTO fifoSchedulerAppsDTO = session.newInstance(FifoSchedulerAppsClusterJ.FifoSchedulerAppsDTO.class);
        
        fifoSchedulerAppsDTO.setapplicationattemptid(hop.getApplicationattemptid_id());
        fifoSchedulerAppsDTO.setficaschedulerappid(hop.getFicaschedulerapp_id());
        fifoSchedulerAppsDTO.setfifoschedulerid(hop.getFifoscheduler_id());
        
        return fifoSchedulerAppsDTO;
    }
    
    private List<HopFifoSchedulerApps> createFifoSchedulerAppsList(List<FifoSchedulerAppsClusterJ.FifoSchedulerAppsDTO> results) {
        List<HopFifoSchedulerApps> fifoSchedulerApps = new ArrayList<HopFifoSchedulerApps>();
        for (FifoSchedulerAppsClusterJ.FifoSchedulerAppsDTO persistable : results) {
            fifoSchedulerApps.add(createFifoSchedulerApps(persistable));
        }
        return fifoSchedulerApps;
    }   
}
