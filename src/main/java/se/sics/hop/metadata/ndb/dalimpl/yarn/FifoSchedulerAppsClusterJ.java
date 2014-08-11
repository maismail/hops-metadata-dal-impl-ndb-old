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
        @Column(name = APPID)
        String getapplicationid();
        void setapplicationid(String applicationid);
        
        @Column(name = SCHEDULERAPPLICATION_ID)
        String getschedulerapplicationid();
        void setschedulerapplicationid(String schedulerapplicationid);
    }
    
    private final ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public void prepare(Collection<HopFifoSchedulerApps> modified, Collection<HopFifoSchedulerApps> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopFifoSchedulerApps hop : removed) {
                    FifoSchedulerAppsClusterJ.FifoSchedulerAppsDTO persistable = session.newInstance(FifoSchedulerAppsClusterJ.FifoSchedulerAppsDTO.class, hop.getApplicationid());
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
    public HopFifoSchedulerApps findByAppId(int applicationid) throws StorageException {
        Session session = connector.obtainSession();
        FifoSchedulerAppsDTO entry = null;
        if (session != null) {
            entry = session.find(FifoSchedulerAppsDTO.class, applicationid);
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
    public List<HopFifoSchedulerApps> getAllByAppId(int appId) throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();

            QueryDomainType<FifoSchedulerAppsClusterJ.FifoSchedulerAppsDTO> dobj = qb.createQueryDefinition(FifoSchedulerAppsClusterJ.FifoSchedulerAppsDTO.class);
            Query<FifoSchedulerAppsClusterJ.FifoSchedulerAppsDTO> query = session.createQuery(dobj);

            List<FifoSchedulerAppsClusterJ.FifoSchedulerAppsDTO> results = query.getResultList();
            return createFifoSchedulerAppsList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }   
    }
    
    private HopFifoSchedulerApps createFifoSchedulerApps(FifoSchedulerAppsClusterJ.FifoSchedulerAppsDTO entry) {
        HopFifoSchedulerApps hop = new HopFifoSchedulerApps(entry.getapplicationid(), entry.getschedulerapplicationid());
        return hop;
    }
    
    private HopFifoSchedulerApps createHopFifoSchedulerApps(FifoSchedulerAppsDTO fifoSchedulerAppsDTO) {
        return new HopFifoSchedulerApps(fifoSchedulerAppsDTO.getapplicationid(), fifoSchedulerAppsDTO.getschedulerapplicationid());
    }

    private FifoSchedulerAppsClusterJ.FifoSchedulerAppsDTO createPersistable(HopFifoSchedulerApps hop, Session session) {
        FifoSchedulerAppsClusterJ.FifoSchedulerAppsDTO fifoSchedulerAppsDTO = session.newInstance(FifoSchedulerAppsClusterJ.FifoSchedulerAppsDTO.class);
        
        fifoSchedulerAppsDTO.setapplicationid(hop.getApplicationid());
        fifoSchedulerAppsDTO.setschedulerapplicationid(hop.getSchedulerapplication_id());
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
