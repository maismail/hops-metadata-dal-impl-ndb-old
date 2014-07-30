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
import se.sics.hop.metadata.hdfs.entity.yarn.HopFiCaSchedulerApp;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.FiCaSchedulerAppDataAccess;
import se.sics.hop.metadata.yarn.tabledef.FiCaSchedulerAppTableDef;
import static se.sics.hop.metadata.yarn.tabledef.FiCaSchedulerAppTableDef.RESOURCELIMIT_ID;

/**
 *
 * @author nickstanogias
 */
public class FiCaSchedulerAppClusterJ implements FiCaSchedulerAppTableDef, FiCaSchedulerAppDataAccess<HopFiCaSchedulerApp> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface FiCaSchedulerAppDTO {

        @PrimaryKey
        @Column(name = ID)
        int getid();
        void setid(int id);

        @Column(name = APPSCHEDULINGINFO_ID)
        int getappschedulinginfoid();
        void setappschedulinginfoid(int appschedulinginfoid);
        
        @Column(name = CURRENTRESERVATION_ID)
        int getcurrentreservationid();
        void setcurrentreservationid(int currentreservationid);
        
        @Column(name = RESOURCELIMIT_ID)
        int getresourcelimitid();
        void setresourcelimitid(int resourcelimitid);
        
        @Column(name = CURRENTCONSUMPTION_ID)
        int getcurrentconsumptionid();
        void setcurrentconsumptionid(int currentconsumptionid);
             
        @Column(name = ISSTOPPED)
        boolean getstopped();
        void setstopped(boolean isstopped);
        
        @Column(name = RMCONTEXT_ID)
        int getrmcontextid();
        void setrmcontextid(int rmcontextid);
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    
    @Override
    public HopFiCaSchedulerApp findById(int id) throws StorageException {
        Session session = connector.obtainSession();
        
        FiCaSchedulerAppClusterJ.FiCaSchedulerAppDTO fiCaSchedulerAppDTO = null;
        if(session !=  null) {
            fiCaSchedulerAppDTO = session.find(FiCaSchedulerAppClusterJ.FiCaSchedulerAppDTO.class, id);
        }
        if(fiCaSchedulerAppDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }
        
        return createHopFiCaSchedulerApp(fiCaSchedulerAppDTO);
    }

    @Override
    public void prepare(Collection<HopFiCaSchedulerApp> modified, Collection<HopFiCaSchedulerApp> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopFiCaSchedulerApp hop : removed) {
                    FiCaSchedulerAppClusterJ.FiCaSchedulerAppDTO persistable = session.newInstance(FiCaSchedulerAppClusterJ.FiCaSchedulerAppDTO.class, hop.getId());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopFiCaSchedulerApp hop : modified) {
                    FiCaSchedulerAppClusterJ.FiCaSchedulerAppDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }    
    }
    
    private HopFiCaSchedulerApp createHopFiCaSchedulerApp(FiCaSchedulerAppDTO fiCaSchedulerAppDTO) {
        return new HopFiCaSchedulerApp(fiCaSchedulerAppDTO.getid(),
                                       fiCaSchedulerAppDTO.getappschedulinginfoid(),
                                       fiCaSchedulerAppDTO.getcurrentreservationid(),
                                       fiCaSchedulerAppDTO.getresourcelimitid(),
                                       fiCaSchedulerAppDTO.getcurrentconsumptionid(),
                                       fiCaSchedulerAppDTO.getstopped(),
                                       fiCaSchedulerAppDTO.getrmcontextid());
    }
    
    private FiCaSchedulerAppClusterJ.FiCaSchedulerAppDTO createPersistable(HopFiCaSchedulerApp hop, Session session) {
        FiCaSchedulerAppClusterJ.FiCaSchedulerAppDTO fiCaSchedulerAppDTO = session.newInstance(FiCaSchedulerAppClusterJ.FiCaSchedulerAppDTO.class);
        
        fiCaSchedulerAppDTO.setid(hop.getId());
        fiCaSchedulerAppDTO.setappschedulinginfoid(hop.getAppschedulinginfo_id());
        fiCaSchedulerAppDTO.setcurrentreservationid(hop.getCurrentreservation_id());
        fiCaSchedulerAppDTO.setresourcelimitid(hop.getResourcelimit_id());
        fiCaSchedulerAppDTO.setcurrentconsumptionid(hop.getCurrentconsumption_id());
        fiCaSchedulerAppDTO.setstopped(hop.isIsstoped());  
        fiCaSchedulerAppDTO.setrmcontextid(hop.getRmcontext_id());
        
        return fiCaSchedulerAppDTO;
    }
    
    
}
