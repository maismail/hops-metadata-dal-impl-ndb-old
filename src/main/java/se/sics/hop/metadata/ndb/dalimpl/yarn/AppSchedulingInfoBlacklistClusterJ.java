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
import se.sics.hop.metadata.hdfs.entity.yarn.HopAppSchedulingInfoBlacklist;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.AppSchedulingInfoBlacklistDataAccess;
import se.sics.hop.metadata.yarn.tabledef.AppSchedulingInfoBlacklistTableDef;

/**
 *
 * @author nickstanogias
 */
public class AppSchedulingInfoBlacklistClusterJ implements AppSchedulingInfoBlacklistTableDef, AppSchedulingInfoBlacklistDataAccess<HopAppSchedulingInfoBlacklist>{

    
    @PersistenceCapable(table = TABLE_NAME)
    public interface AppSchedulingInfoBlacklistDTO {

        @PrimaryKey
        @Column(name = APPSCHEDULINGINFO_ID)
        String getappschedulinginfoid();
        void setappschedulinginfoid(String appschedulinginfoid);

        @Column(name = BLACKLISTED)
        String getblacklisted();
        void setblacklisted(String blacklisted);

    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public HopAppSchedulingInfoBlacklist findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO appSchedulingInfoBlacklistDTO = null;
        if (session != null) {
            appSchedulingInfoBlacklistDTO = session.find(AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO.class, id);
        }
        if (appSchedulingInfoBlacklistDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopAppSchedulingInfoBlacklist(appSchedulingInfoBlacklistDTO);
    }

    @Override
    public void prepare(Collection<HopAppSchedulingInfoBlacklist> modified, Collection<HopAppSchedulingInfoBlacklist> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopAppSchedulingInfoBlacklist hop : removed) {
                    AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO persistable = session.newInstance(AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO.class, hop.getAppschedulinginfo_id());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopAppSchedulingInfoBlacklist hop : modified) {
                    AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    private HopAppSchedulingInfoBlacklist createHopAppSchedulingInfoBlacklist(AppSchedulingInfoBlacklistDTO appSchedulingInfoBlacklistDTO) {
        return new HopAppSchedulingInfoBlacklist(appSchedulingInfoBlacklistDTO.getappschedulinginfoid(),
                                                appSchedulingInfoBlacklistDTO.getblacklisted());
    }

    private AppSchedulingInfoBlacklistDTO createPersistable(HopAppSchedulingInfoBlacklist hop, Session session) {
        AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO appSchedulingInfoBlacklistDTO = session.newInstance(AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO.class);
        
        appSchedulingInfoBlacklistDTO.setappschedulinginfoid(hop.getAppschedulinginfo_id());
        appSchedulingInfoBlacklistDTO.setblacklisted(hop.getBlacklisted());
        
        return appSchedulingInfoBlacklistDTO;
    }
    
}
