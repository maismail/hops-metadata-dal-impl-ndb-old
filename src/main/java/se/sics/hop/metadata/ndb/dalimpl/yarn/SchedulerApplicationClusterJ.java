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
import se.sics.hop.metadata.hdfs.entity.yarn.HopSchedulerApplication;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.SchedulerApplicationDataAccess;
import se.sics.hop.metadata.yarn.tabledef.SchedulerApplicationTableDef;

/**
 *
 * @author Nikos Stanogias <niksta@sics.se>
 */
public class SchedulerApplicationClusterJ implements SchedulerApplicationTableDef, SchedulerApplicationDataAccess<HopSchedulerApplication> {


    @PersistenceCapable(table = TABLE_NAME)
    public interface SchedulerApplicationDTO {

    @PrimaryKey
    @Column(name = ID)
    int getid();
    void setid(int id);

    @Column(name = USER)
    String getuser();
    void setuser(String user);
        
    @Column(name = CURRENTATTEMPT_ID)
    int getcurrentattemptid();
    void setcurrentattemptid(int currentattemptid);
        
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public HopSchedulerApplication findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        SchedulerApplicationClusterJ.SchedulerApplicationDTO schedulerApplicationDTO = null;
        if (session != null) {
            schedulerApplicationDTO = session.find(SchedulerApplicationClusterJ.SchedulerApplicationDTO.class, id);
        }
        if (schedulerApplicationDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopSchedulerApplication(schedulerApplicationDTO);
    }

    @Override
    public void prepare(Collection<HopSchedulerApplication> modified, Collection<HopSchedulerApplication> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopSchedulerApplication hop : removed) {
                    SchedulerApplicationClusterJ.SchedulerApplicationDTO persistable = session.newInstance(SchedulerApplicationClusterJ.SchedulerApplicationDTO.class, hop.getId());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopSchedulerApplication hop : modified) {
                    SchedulerApplicationClusterJ.SchedulerApplicationDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    private HopSchedulerApplication createHopSchedulerApplication(SchedulerApplicationDTO schedulerApplicationDTO) {
        return new HopSchedulerApplication(schedulerApplicationDTO.getid(), schedulerApplicationDTO.getuser(), schedulerApplicationDTO.getcurrentattemptid());
    }

    private SchedulerApplicationDTO createPersistable(HopSchedulerApplication hop, Session session) {
        SchedulerApplicationClusterJ.SchedulerApplicationDTO schedulerApplicationDTO = session.newInstance(SchedulerApplicationClusterJ.SchedulerApplicationDTO.class);
        
        schedulerApplicationDTO.setid(hop.getId());
        schedulerApplicationDTO.setuser(hop.getUser());
        schedulerApplicationDTO.setcurrentattemptid(hop.getCurrentattempt_id());
        return schedulerApplicationDTO;
    }
    
    @Override
    public void createEntry(HopSchedulerApplication SchedulerApp) throws StorageException {
        Session session = connector.obtainSession();
        session.savePersistent(createPersistable(SchedulerApp, session));
    }   
}
