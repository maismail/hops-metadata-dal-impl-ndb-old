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
import se.sics.hop.metadata.hdfs.entity.yarn.HopRMApp;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.RMAppDataAccess;
import se.sics.hop.metadata.yarn.tabledef.RMAppTableDef;

public class RMAppClusterJ implements RMAppTableDef, RMAppDataAccess<HopRMApp>{
    
    @PersistenceCapable(table = TABLE_NAME)
    public interface RMAppDTO {

        @PrimaryKey
        @Column(name = ID)
        int getid();
        void setid(int id);

        @Column(name = APPLICATIONID_ID)
        int getapplicationidid();
        void setapplicationidid(int applicationidid);
        
        @Column(name = RMCONTEXT_ID)
        int getrmcontextid();
        void setrmcontextid(int rmcontextid);
        
        @Column(name = USER)
        String getuser();
        void setuser(String user);
        
        @Column(name = NAME)
        String getname();
        void setname(String name);
        
        @Column(name = APPSUBMISSIONCTX_ID)
        int getappsubmissionctxid();
        void setappsubmissionctxid(int appsubmissionctxid);
        
        @Column(name = SCHEDULER_ID)
        int getschedulerid();
        void setschedulerid(int schedulerid);
        
        @Column(name = APPMASTERSERVICE_ID)
        int getappmasterserviceid();
        void setappmasterserviceid(int appmasterserviceid);
        
        @Column(name = DIAGNOSTICS)
        String getdiagnostics();
        void setdiagnostics(String diagnostics);
        
        @Column(name = MAXAPPATTEMPTS)
        int getmaxappattempts();
        void setmaxappattempts(int maxappattempts);
        
        @Column(name = SUBMITTIME)
        long getsubmittime();
        void setsubmittime(long submittime);
        
        @Column(name = APPLICATIONTYPE)
        String getapplicationtype();
        void setapplicationtype(String applicationtype);
        
        @Column(name = STARTTIME)
        long getstarttime();
        void setstarttime(long starttime);
        
        @Column(name = FINISHTIME)
        long getfinishtime();
        void setfinishtime(long finishtime);
        
        @Column(name = CURRENTATTEMPT_ID)
        int getcurrentattemptid();
        void setcurrentattemptid(int currentattemptid);
        
        @Column(name = ISAPPREMOVALREQUESTSENT)
        boolean isappremovalrequestsent();
        void setappremovalrequestsent(boolean isappremovalrequestsent);      
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopRMApp findById(int id) throws StorageException {
        HopsSession session = connector.obtainSession();

        RMAppClusterJ.RMAppDTO rMAPDTO = null;
        if (session != null) {
            rMAPDTO = session.find(RMAppClusterJ.RMAppDTO.class, id);
        }
        if (rMAPDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopRMApp(rMAPDTO);
    }

    @Override
    public void prepare(Collection<HopRMApp> modified, Collection<HopRMApp> removed) throws StorageException {
        HopsSession session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopRMApp hop : removed) {
                    RMAppClusterJ.RMAppDTO persistable = session.newInstance(RMAppClusterJ.RMAppDTO.class, hop.getId());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopRMApp hop : modified) {
                    RMAppClusterJ.RMAppDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    private HopRMApp createHopRMApp(RMAppDTO rMAPDTO) {
        return new HopRMApp(rMAPDTO.getid(),
                            rMAPDTO.getapplicationidid(),
                            rMAPDTO.getrmcontextid(),
                            rMAPDTO.getuser(),
                            rMAPDTO.getname(),
                            rMAPDTO.getappsubmissionctxid(),
                            rMAPDTO.getschedulerid(),
                            rMAPDTO.getappmasterserviceid(),
                            rMAPDTO.getdiagnostics(),
                            rMAPDTO.getmaxappattempts(),
                            rMAPDTO.getsubmittime(),
                            rMAPDTO.getapplicationtype(),
                            rMAPDTO.getstarttime(),
                            rMAPDTO.getfinishtime(),
                            rMAPDTO.getcurrentattemptid(),
                            rMAPDTO.isappremovalrequestsent());
    }
    
    private RMAppDTO createPersistable(HopRMApp hop, HopsSession session) throws StorageException {
        RMAppClusterJ.RMAppDTO rMAPDTO = session.newInstance(RMAppClusterJ.RMAppDTO.class);
        
        rMAPDTO.setapplicationidid(hop.getApplicationidid());
        rMAPDTO.setapplicationtype(hop.getApplicationtype());
        rMAPDTO.setappmasterserviceid(hop.getAppmasterserviceid());
        rMAPDTO.setappremovalrequestsent(hop.isIsappremovalrequestsent());
        rMAPDTO.setappsubmissionctxid(hop.getAppsubmissionctxid());
        rMAPDTO.setcurrentattemptid(hop.getCurrentattemptid());
        rMAPDTO.setdiagnostics(hop.getDiagnostics());
        rMAPDTO.setfinishtime(hop.getFinishtime());
        rMAPDTO.setid(hop.getId());
        rMAPDTO.setmaxappattempts(hop.getMaxappattempts());
        rMAPDTO.setname(hop.getName());
        rMAPDTO.setrmcontextid(hop.getRmcontextid());
        rMAPDTO.setschedulerid(hop.getSchedulerid());
        rMAPDTO.setstarttime(hop.getStarttime());
        rMAPDTO.setsubmittime(hop.getSubmittime());
        rMAPDTO.setuser(hop.getUser());
        
        return rMAPDTO;
    }
    
}
