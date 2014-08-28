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
import java.sql.SQLException;
import java.util.Collection;
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
        String getappschedulinginfoid();
        void setappschedulinginfoid(String appschedulinginfoid);

        @Column(name = PRIORITY_ID)
        int getpriorityid();
        void setpriorityid(int priorityid);
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public HopAppSchedulingInfoPriorities findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        AppSchedulingInfoPrioritiesClusterJ.AppSchedulingInfoPrioritiesDTO appSchedulingInfoPrioritiesDTO = null;
        if (session != null) {
            appSchedulingInfoPrioritiesDTO = session.find(AppSchedulingInfoPrioritiesClusterJ.AppSchedulingInfoPrioritiesDTO.class, id);
        }
        if (appSchedulingInfoPrioritiesDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopAppSchedulingInfoPriorities(appSchedulingInfoPrioritiesDTO);
    }

    @Override
    public void prepare(Collection<HopAppSchedulingInfoPriorities> modified, Collection<HopAppSchedulingInfoPriorities> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopAppSchedulingInfoPriorities hop : removed) {
                    AppSchedulingInfoPrioritiesClusterJ.AppSchedulingInfoPrioritiesDTO persistable = session.newInstance(AppSchedulingInfoPrioritiesClusterJ.AppSchedulingInfoPrioritiesDTO.class, hop.getAppschedulinginfo_id());
                    session.deletePersistent(persistable);
                }
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
        return new HopAppSchedulingInfoPriorities(appSchedulingInfoPrioritiesDTO.getappschedulinginfoid(),
                                                appSchedulingInfoPrioritiesDTO.getpriorityid());
    }

    private AppSchedulingInfoPrioritiesDTO createPersistable(HopAppSchedulingInfoPriorities hop, Session session) {
        AppSchedulingInfoPrioritiesClusterJ.AppSchedulingInfoPrioritiesDTO appSchedulingInfoPrioritiesDTO = session.newInstance(AppSchedulingInfoPrioritiesClusterJ.AppSchedulingInfoPrioritiesDTO.class);
        
        appSchedulingInfoPrioritiesDTO.setappschedulinginfoid(hop.getAppschedulinginfo_id());
        appSchedulingInfoPrioritiesDTO.setpriorityid(hop.getPriority_id());
        
        return appSchedulingInfoPrioritiesDTO;
    }
}
