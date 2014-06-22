

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
import se.sics.hop.metadata.hdfs.entity.yarn.HopAppSchedulingInfoRequestsValues;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.AppSchedulingInfoRequestsValuesDataAccess;
import se.sics.hop.metadata.yarn.tabledef.AppSchedulingInfoRequestsValuesTableDef;

/**
 *
 * @author nickstanogias
 */
public class AppSchedulingInfoRequestsValuesClusterJ implements AppSchedulingInfoRequestsValuesTableDef, AppSchedulingInfoRequestsValuesDataAccess<HopAppSchedulingInfoRequestsValues> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface AppSchedulingInfoRequestsValuesDTO {

        @PrimaryKey
        @Column(name = APPSCHEDULINGINFO_REQUESTS_ID)
        int getappschedulinginforequestsid();
        void setappschedulinginforequestsid(int appschedulinginforequestsid);

        @Column(name = APPSCHEDULINGINFO_PRIORITY)
        int getappschedulinginfopriority();
        void setappschedulinginfopriority(int appschedulinginfopriority);
        
        @Column(name = NAME)
        String getname();
        void setname(String name);
        
        @Column(name = RESOURCEREQUEST_ID)
        int getresourcerequestid();
        void setresourcerequestid(int resourcerequestid);
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public HopAppSchedulingInfoRequestsValues findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        AppSchedulingInfoRequestsValuesClusterJ.AppSchedulingInfoRequestsValuesDTO appSchedulingInfoRequestsValuesDTO = null;
        if (session != null) {
            appSchedulingInfoRequestsValuesDTO = session.find(AppSchedulingInfoRequestsValuesClusterJ.AppSchedulingInfoRequestsValuesDTO.class, id);
        }
        if (appSchedulingInfoRequestsValuesDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopAppSchedulingInfoRequestsValues(appSchedulingInfoRequestsValuesDTO);
    }

    @Override
    public void prepare(Collection<HopAppSchedulingInfoRequestsValues> modified, Collection<HopAppSchedulingInfoRequestsValues> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopAppSchedulingInfoRequestsValues hop : removed) {
                    AppSchedulingInfoRequestsValuesClusterJ.AppSchedulingInfoRequestsValuesDTO persistable = session.newInstance(AppSchedulingInfoRequestsValuesClusterJ.AppSchedulingInfoRequestsValuesDTO.class, hop.getAppschedulinginfo_requests_id());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopAppSchedulingInfoRequestsValues hop : modified) {
                    AppSchedulingInfoRequestsValuesClusterJ.AppSchedulingInfoRequestsValuesDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    private HopAppSchedulingInfoRequestsValues createHopAppSchedulingInfoRequestsValues(AppSchedulingInfoRequestsValuesDTO appSchedulingInfoRequestsValuesDTO) {
        return new HopAppSchedulingInfoRequestsValues(appSchedulingInfoRequestsValuesDTO.getappschedulinginforequestsid(),
                                                      appSchedulingInfoRequestsValuesDTO.getappschedulinginfopriority(),
                                                        appSchedulingInfoRequestsValuesDTO.getname(),
                                                        appSchedulingInfoRequestsValuesDTO.getresourcerequestid());
    }

    private AppSchedulingInfoRequestsValuesDTO createPersistable(HopAppSchedulingInfoRequestsValues hop, Session session) {
        AppSchedulingInfoRequestsValuesClusterJ.AppSchedulingInfoRequestsValuesDTO appSchedulingInfoRequestsValuesDTO = session.newInstance(AppSchedulingInfoRequestsValuesClusterJ.AppSchedulingInfoRequestsValuesDTO.class);
        
        appSchedulingInfoRequestsValuesDTO.setappschedulinginfopriority(hop.getAppschedulinginfo_priority());
        appSchedulingInfoRequestsValuesDTO.setappschedulinginforequestsid(hop.getAppschedulinginfo_requests_id());
        appSchedulingInfoRequestsValuesDTO.setname(hop.getName());
        appSchedulingInfoRequestsValuesDTO.setresourcerequestid(hop.getResourcerequest_id());
        
        return appSchedulingInfoRequestsValuesDTO;
    }
    
}
