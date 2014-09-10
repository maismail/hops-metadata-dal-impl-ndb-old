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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopAppSchedulingInfoPriorities;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.mysqlserver.MysqlServerConnector;
import se.sics.hop.metadata.yarn.dal.AppSchedulingInfoPrioritiesDataAccess;
import se.sics.hop.metadata.yarn.tabledef.AppSchedulingInfoPrioritiesTableDef;
import static se.sics.hop.metadata.yarn.tabledef.AppSchedulingInfoPrioritiesTableDef.TABLE_NAME;

/**
 *
 * @author Nikos Stanogias <niksta@sics.se>
 */
public class AppSchedulingInfoPrioritiesClusterJ implements AppSchedulingInfoPrioritiesTableDef, AppSchedulingInfoPrioritiesDataAccess<HopAppSchedulingInfoPriorities>{


    @PersistenceCapable(table = TABLE_NAME)
    public interface AppSchedulingInfoPrioritiesDTO {

        @PrimaryKey
        @Column(name = APPSCHEDULINGINFO_ID)
        String getappschedulinginfo_id();
        void setappschedulinginfo_id(String appschedulinginfo_id);

        @Column(name = PRIORITY_ID)
        int getpriority_id();
        void setpriority_id(int priority_id);
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public List<HopAppSchedulingInfoPriorities> findById(String id) throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();

            QueryDomainType<AppSchedulingInfoPrioritiesClusterJ.AppSchedulingInfoPrioritiesDTO> dobj = qb.createQueryDefinition(AppSchedulingInfoPrioritiesClusterJ.AppSchedulingInfoPrioritiesDTO.class);
            Predicate pred1 = dobj.get("appschedulinginfo_id").equal(dobj.param("appschedulinginfo_id"));
            dobj.where(pred1);
            Query<AppSchedulingInfoPrioritiesClusterJ.AppSchedulingInfoPrioritiesDTO> query = session.createQuery(dobj);
            query.setParameter("appschedulinginfo_id", id);

            List<AppSchedulingInfoPrioritiesClusterJ.AppSchedulingInfoPrioritiesDTO> results = query.getResultList();
            return createPrioritiesList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void prepare(Collection<HopAppSchedulingInfoPriorities> modified, Collection<HopAppSchedulingInfoPriorities> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                List<AppSchedulingInfoPrioritiesClusterJ.AppSchedulingInfoPrioritiesDTO> toRemove = new ArrayList<AppSchedulingInfoPrioritiesClusterJ.AppSchedulingInfoPrioritiesDTO>();
                for (HopAppSchedulingInfoPriorities hop : removed) {
                    Object[] objarr = new Object[2];
                    objarr[0] = hop.getAppschedulinginfo_id();
                    objarr[1] = hop.getPriority_id();
                    toRemove.add(session.newInstance(AppSchedulingInfoPrioritiesClusterJ.AppSchedulingInfoPrioritiesDTO.class, objarr));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                for (HopAppSchedulingInfoPriorities hop : modified) {
                    AppSchedulingInfoPrioritiesClusterJ.AppSchedulingInfoPrioritiesDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    @Override
    public void truncateTable(){
        try {  
            MysqlServerConnector.truncateTable(AppSchedulingInfoPrioritiesTableDef.TABLE_NAME);
        } catch (StorageException ex) {
            Logger.getLogger(AppSchedulingInfoPrioritiesClusterJ.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(AppSchedulingInfoPrioritiesClusterJ.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    private HopAppSchedulingInfoPriorities createHopAppSchedulingInfoPriorities(AppSchedulingInfoPrioritiesDTO appSchedulingInfoPrioritiesDTO) {
        return new HopAppSchedulingInfoPriorities(appSchedulingInfoPrioritiesDTO.getappschedulinginfo_id(),
                                                appSchedulingInfoPrioritiesDTO.getpriority_id());
    }

    private AppSchedulingInfoPrioritiesDTO createPersistable(HopAppSchedulingInfoPriorities hop, Session session) {
        AppSchedulingInfoPrioritiesClusterJ.AppSchedulingInfoPrioritiesDTO appSchedulingInfoPrioritiesDTO = session.newInstance(AppSchedulingInfoPrioritiesClusterJ.AppSchedulingInfoPrioritiesDTO.class);
        
        appSchedulingInfoPrioritiesDTO.setappschedulinginfo_id(hop.getAppschedulinginfo_id());
        appSchedulingInfoPrioritiesDTO.setpriority_id(hop.getPriority_id());
        
        return appSchedulingInfoPrioritiesDTO;
    }
    
    private List<HopAppSchedulingInfoPriorities> createPrioritiesList(List<AppSchedulingInfoPrioritiesClusterJ.AppSchedulingInfoPrioritiesDTO> results) {
        List<HopAppSchedulingInfoPriorities> priorities = new ArrayList<HopAppSchedulingInfoPriorities>();
        for (AppSchedulingInfoPrioritiesClusterJ.AppSchedulingInfoPrioritiesDTO persistable : results) {
            priorities.add(createHopAppSchedulingInfoPriorities(persistable));
        }
        return priorities;
    }
}
