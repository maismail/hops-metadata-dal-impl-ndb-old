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
import se.sics.hop.metadata.hdfs.entity.yarn.HopAppSchedulingInfo;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.AppSchedulingInfoDataAccess;
import se.sics.hop.metadata.yarn.tabledef.AppSchedulingInfoTableDef;

/**
 *
 * @author nickstanogias
 */
public class AppSchedulingInfoClusterJ implements AppSchedulingInfoTableDef, AppSchedulingInfoDataAccess<HopAppSchedulingInfo>{


    @PersistenceCapable(table = TABLE_NAME)
    public interface AppSchedulingInfoDTO {

        @PrimaryKey
        @Column(name = ID)
        int getid();
        void setid(int id);

        @Column(name = APPLICATIONATTEMPTID_ID)
        int getapplicationattemptidid();
        void setapplicationattemptidid(int applicationattemptidid);
        
        @Column(name = QUEUE_NAME)
        String getqueuename();
        void setqueuename(String queuename);
        
        @Column(name = USER)
        String getuser();
        void setuser(String user);
        
        @Column(name = CONTAINERIDCOUNTER)
        String getcontaineridcounter();
        void setcontaineridcounter(String containeridcounter);
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public HopAppSchedulingInfo findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        AppSchedulingInfoClusterJ.AppSchedulingInfoDTO appSchedulingInfoDTO = null;
        if (session != null) {
            appSchedulingInfoDTO = session.find(AppSchedulingInfoClusterJ.AppSchedulingInfoDTO.class, id);
        }
        if (appSchedulingInfoDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopAppSchedulingInfo(appSchedulingInfoDTO);
    }
    
    @Override
    public void prepare(Collection<HopAppSchedulingInfo> modified, Collection<HopAppSchedulingInfo> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopAppSchedulingInfo hop : removed) {
                    AppSchedulingInfoClusterJ.AppSchedulingInfoDTO persistable = session.newInstance(AppSchedulingInfoClusterJ.AppSchedulingInfoDTO.class, hop.getId());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopAppSchedulingInfo hop : modified) {
                    AppSchedulingInfoClusterJ.AppSchedulingInfoDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    private HopAppSchedulingInfo createHopAppSchedulingInfo(AppSchedulingInfoDTO appSchedulingInfoDTO) {
        return new HopAppSchedulingInfo(appSchedulingInfoDTO.getid(),
                                        appSchedulingInfoDTO.getapplicationattemptidid(),
                                        appSchedulingInfoDTO.getqueuename(),
                                        appSchedulingInfoDTO.getuser(),
                                        appSchedulingInfoDTO.getcontaineridcounter());
    }

    private AppSchedulingInfoDTO createPersistable(HopAppSchedulingInfo hop, Session session) {
        AppSchedulingInfoClusterJ.AppSchedulingInfoDTO appSchedulingInfoDTO = session.newInstance(AppSchedulingInfoClusterJ.AppSchedulingInfoDTO.class);
        
        appSchedulingInfoDTO.setapplicationattemptidid(hop.getApplicationattemptid_id());
        appSchedulingInfoDTO.setcontaineridcounter(hop.getContaineridcounter());
        appSchedulingInfoDTO.setid(hop.getId());
        appSchedulingInfoDTO.setqueuename(hop.getQueue_name());
        appSchedulingInfoDTO.setuser(hop.getUser());
        
        return appSchedulingInfoDTO;
    }
    
}
