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
import se.sics.hop.metadata.hdfs.entity.yarn.HopAppState;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.AppStateDataAccess;
import se.sics.hop.metadata.yarn.tabledef.AppstateTableDef;

/**
 *
 * @author nickstanogias
 */
public class AppStateClusterJ implements AppstateTableDef, AppStateDataAccess<HopAppState>{


    @PersistenceCapable(table = TABLE_NAME)
    public interface AppStateDTO {

        @PrimaryKey
        @Column(name = APPLICATIONID)
        String getapplicationid();
        void setappliactionid(String applicationid);

        @Column(name = APPSTATE)
        byte[] getappstate();
        void setappstate(byte[] appstate);
        
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public HopAppState findByApplicationId(int id) throws StorageException {
        Session session = connector.obtainSession();

        AppStateClusterJ.AppStateDTO appStateDTO = null;
        if (session != null) {
            appStateDTO = session.find(AppStateClusterJ.AppStateDTO.class, id);
        }
        if (appStateDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopAppState(appStateDTO);
    }

    @Override
    public void prepare(Collection<HopAppState> modified, Collection<HopAppState> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopAppState hop : removed) {
                    AppStateClusterJ.AppStateDTO persistable = session.newInstance(AppStateClusterJ.AppStateDTO.class, hop.getApplicationid());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopAppState hop : modified) {
                    AppStateClusterJ.AppStateDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    private HopAppState createHopAppState(AppStateDTO appStateDTO) {
        return new HopAppState(appStateDTO.getapplicationid(),
                                appStateDTO.getappstate());
    }
    
    private AppStateDTO createPersistable(HopAppState hop, Session session) {
        AppStateClusterJ.AppStateDTO appStateDTO = session.newInstance(AppStateClusterJ.AppStateDTO.class);
        
        appStateDTO.setappliactionid(hop.getApplicationid());
        appStateDTO.setappstate(hop.getAppstate());
        
        return appStateDTO;
    }
}
