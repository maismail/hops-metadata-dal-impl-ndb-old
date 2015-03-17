/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopRMAppAttempt;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.RMAppAttemptDataAccess;
import se.sics.hop.metadata.yarn.tabledef.RMAppAttemptTableDef;

public class RMAppAttemptClusterJ implements RMAppAttemptTableDef, RMAppAttemptDataAccess<HopRMAppAttempt>{


    @PersistenceCapable(table = TABLE_NAME)
    public interface RMAppAttemptDTO {

        @PrimaryKey
        @Column(name = ID)
        int getid();
        void setid(int id);

        @Column(name = RMCONTEXT_ID)
        int getrmcontextid();
        void setrmcontextid(int rmcontextid);
        
        @Column(name = SCHEDULER_ID)
        int getschedulerid();
        void setschedulerid(int schedulerid);
        
        @Column(name = APPMASTERSERVICE_ID)
        int getappmasterserviceid();
        void setappmasterserviceid(int appmasterserviceid);
        
        @Column(name = APPATTEMPTID_ID)
        int getappattemptidid();
        void setappattemptidid(int appattemptidid);
        
        @Column(name = APPSUBMISSIONCTX)
        int getappsubmissionctx();
        void setappsubmissionctx(int appsubmissionctx);
        
        @Column(name = CONTAINER_ID)
        int getcontainerid();
        void setcontainerid(int containerid);
        
        @Column(name = PROGRESS)
        float getprogress();
        void setprogress(float progress);
        
        @Column(name = HOST)
        String gethost();
        void sethost(String host);
        
        @Column(name = RPCPORT)
        int getrpcport();
        void setrpcport(int rpcport);
        
        @Column(name = ORIGTRACKINGURL)
        String getorigtrackingurl();
        void setorigtrackingurl(String origtrackingurl);
        
        @Column(name = PROXIEDTRACKINGURL)
        String getproxiedtrackingurl();
        void setproxiedtrackingurl(String proxiedtrackingurl);
        
        @Column(name = STARTTIME)
        long getstarttime();
        void setstarttime(long starttime);
        
        @Column(name = FINALSTATUS)
        String getfinalstatus();
        void setfinalstatus(String finalstatus);
        
        @Column(name = DIAGNOSTICS)
        String getdiagnostics();
        void setdiagnostics(String diagnostics);
        
        @Column(name = USER)
        String getuser();
        void setuser(String user);
  
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public HopRMAppAttempt findById(int id) throws StorageException {
        HopsSession session = connector.obtainSession();
        
        RMAppAttemptClusterJ.RMAppAttemptDTO rMAppAttemptDTO = null;
        if (session != null) {
            rMAppAttemptDTO = session.find(RMAppAttemptClusterJ.RMAppAttemptDTO.class, id);
        }
        if (rMAppAttemptDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopRMAppAttempt(rMAppAttemptDTO);
    }

    @Override
    public void prepare(Collection<HopRMAppAttempt> modified, Collection<HopRMAppAttempt> removed) throws StorageException {
        HopsSession session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopRMAppAttempt hop : removed) {
                    RMAppAttemptClusterJ.RMAppAttemptDTO persistable = session.newInstance(RMAppAttemptClusterJ.RMAppAttemptDTO.class, hop.getId());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopRMAppAttempt hop : modified) {
                    RMAppAttemptClusterJ.RMAppAttemptDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    private HopRMAppAttempt createHopRMAppAttempt(RMAppAttemptDTO rMAppAttemptDTO) {
        return new HopRMAppAttempt(rMAppAttemptDTO.getid(),
                                    rMAppAttemptDTO.getrmcontextid(),
                                    rMAppAttemptDTO.getschedulerid(),
                                    rMAppAttemptDTO.getappmasterserviceid(),
                                    rMAppAttemptDTO.getappattemptidid(),
                                    rMAppAttemptDTO.getappsubmissionctx(),
                                    rMAppAttemptDTO.getcontainerid(),
                                    rMAppAttemptDTO.getprogress(),
                                    rMAppAttemptDTO.gethost(),
                                    rMAppAttemptDTO.getrpcport(),
                                    rMAppAttemptDTO.getorigtrackingurl(),
                                    rMAppAttemptDTO.getproxiedtrackingurl(),
                                    rMAppAttemptDTO.getstarttime(),
                                    rMAppAttemptDTO.getfinalstatus(),
                                    rMAppAttemptDTO.getdiagnostics(),
                                    rMAppAttemptDTO.getuser());
    }

    private RMAppAttemptDTO createPersistable(HopRMAppAttempt hop, HopsSession session) throws StorageException {
         RMAppAttemptClusterJ.RMAppAttemptDTO rMAppAttemptDTO = session.newInstance(RMAppAttemptClusterJ.RMAppAttemptDTO.class);
         
         rMAppAttemptDTO.setappattemptidid(hop.getAppattemptidid());
         rMAppAttemptDTO.setappmasterserviceid(hop.getAppmasterserviceid());
         rMAppAttemptDTO.setappsubmissionctx(hop.getAppsubmissionctx());
         rMAppAttemptDTO.setcontainerid(hop.getContainerid());
         rMAppAttemptDTO.setdiagnostics(hop.getDiagnostics());
         rMAppAttemptDTO.setfinalstatus(hop.getFinalstatus());
         rMAppAttemptDTO.sethost(hop.getHost());
         rMAppAttemptDTO.setid(hop.getId());
         rMAppAttemptDTO.setorigtrackingurl(hop.getOrigtrackingurl());
         rMAppAttemptDTO.setprogress(hop.getProgress());
         rMAppAttemptDTO.setproxiedtrackingurl(hop.getProxiedtrackingurl());
         rMAppAttemptDTO.setrmcontextid(hop.getRmcontextid());
         rMAppAttemptDTO.setrpcport(hop.getRpcport());
         rMAppAttemptDTO.setschedulerid(hop.getSchedulerid());
         rMAppAttemptDTO.setstarttime(hop.getStarttime());
         rMAppAttemptDTO.setuser(hop.getUser());
         
         return rMAppAttemptDTO;
    }
    
}
