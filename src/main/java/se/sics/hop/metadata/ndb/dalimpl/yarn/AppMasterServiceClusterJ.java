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
import se.sics.hop.metadata.hdfs.entity.yarn.HopAppMasterService;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.AppMasterServiceDataAccess;
import se.sics.hop.metadata.yarn.tabledef.AppMasterServiceTableDef;

/**
 *
 * @author nickstanogias
 */
public class AppMasterServiceClusterJ implements AppMasterServiceTableDef, AppMasterServiceDataAccess<HopAppMasterService> {
    
    @PersistenceCapable(table = TABLE_NAME)
    public interface AppMasterServiceDTO {

        @PrimaryKey
        @Column(name = ID)
        int getid();
        void setid(int id);

        @Column(name = SCHEDULER_ID)
        int getschedulerid();
        void setschedulerid(int schedulerid);
        
        @Column(name = RMCONTEXT_ID)
        int getrmcontextid();
        void setrmcontextid(int rmcontextid);
        
        @Column(name = RESYNC)
        int getresync();
        void setresync(int resync);
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public HopAppMasterService findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        AppMasterServiceClusterJ.AppMasterServiceDTO appMasterServiceDTO = null;
        if (session != null) {
            appMasterServiceDTO = session.find(AppMasterServiceClusterJ.AppMasterServiceDTO.class, id);
        }
        if (appMasterServiceDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopAppMasterService(appMasterServiceDTO);
    }

    @Override
    public void prepare(Collection<HopAppMasterService> modified, Collection<HopAppMasterService> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopAppMasterService hop : removed) {
                    AppMasterServiceClusterJ.AppMasterServiceDTO persistable = session.newInstance(AppMasterServiceClusterJ.AppMasterServiceDTO.class, hop.getId());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopAppMasterService hop : modified) {
                    AppMasterServiceClusterJ.AppMasterServiceDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    private HopAppMasterService createHopAppMasterService(AppMasterServiceDTO appMasterServiceDTO) {
        return new HopAppMasterService(appMasterServiceDTO.getid(), 
                                       appMasterServiceDTO.getschedulerid(),
                                       appMasterServiceDTO.getrmcontextid(),
                                        appMasterServiceDTO.getresync());  
    }
    
    private AppMasterServiceClusterJ.AppMasterServiceDTO createPersistable(HopAppMasterService hop, Session session) {
        AppMasterServiceClusterJ.AppMasterServiceDTO appMasterServiceDTO = session.newInstance(AppMasterServiceClusterJ.AppMasterServiceDTO.class);
        
        appMasterServiceDTO.setid(hop.getId());
        appMasterServiceDTO.setrmcontextid(hop.getRmcontext_id());
        appMasterServiceDTO.setschedulerid(hop.getScheduler_id());
        appMasterServiceDTO.setresync(hop.getResync());
        return appMasterServiceDTO;
    }
    
}
