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
import se.sics.hop.metadata.hdfs.entity.yarn.HopApplicationAttemptState;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.ApplicationAttemptStateDataAccess;
import se.sics.hop.metadata.yarn.tabledef.ApplicationAttemptStateTableDef;
import static se.sics.hop.metadata.yarn.tabledef.ApplicationAttemptStateTableDef.APPLICATIONATTEMPTID;

/**
 *
 * @author nickstanogias
 */
public class ApplicationAttemptStateClusterJ implements ApplicationAttemptStateTableDef, ApplicationAttemptStateDataAccess<HopApplicationAttemptState>{

    @PersistenceCapable(table = TABLE_NAME)
    public interface ApplicationAttemptStateDTO {

        @PrimaryKey
        @Column(name = APPLICATIONID)
        int getapplicationid();
        void setappliactionid(int applicationid);

        @Column(name = APPLICATIONATTEMPTID)
        int getapplicationattemptid();
        void setapplicationattemptid(int applicationattemptid);
        
        @Column(name = APPLICATIONATTEMPTSTATE)
        byte[] getapplicationattemptstate();
        void setapplicationattemptstate(byte[] applicationattemptstate);
        
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopApplicationAttemptState findEntry(int applicationid, int applicationattemptid) throws StorageException {
        Session session = connector.obtainSession();
        Object[] objarr = new Object[2];
        objarr[0] = applicationid;
        objarr[1] = applicationattemptid;
        ApplicationAttemptStateClusterJ.ApplicationAttemptStateDTO entry = null;
        if (session != null) {
            entry = session.find(ApplicationAttemptStateClusterJ.ApplicationAttemptStateDTO.class, objarr);
        }
        if (entry == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }
        
        return createApplicationAttemptState(entry);
    }

    @Override
    public void createApplicationAttemptStateEntry(HopApplicationAttemptState entry) throws StorageException {
        Session session = connector.obtainSession();
        session.savePersistent(createPersistable(entry, session));
    }   

    @Override
    public void prepare(Collection<HopApplicationAttemptState> modified, Collection<HopApplicationAttemptState> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopApplicationAttemptState hop : removed) {
                    Object[] objarr = new Object[2];
                    objarr[0] = hop.getApplicationid();
                    objarr[1] = hop.getApplicationattemptid();
                    ApplicationAttemptStateClusterJ.ApplicationAttemptStateDTO persistable = session.newInstance(ApplicationAttemptStateClusterJ.ApplicationAttemptStateDTO.class, objarr);
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopApplicationAttemptState hop : modified) {
                    ApplicationAttemptStateClusterJ.ApplicationAttemptStateDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    private HopApplicationAttemptState createApplicationAttemptState(ApplicationAttemptStateDTO entry) {
        return new HopApplicationAttemptState(entry.getapplicationid(),
                                              entry.getapplicationattemptid(),
                                              entry.getapplicationattemptstate());
    }

    private ApplicationAttemptStateDTO createPersistable(HopApplicationAttemptState hop, Session session) {
        ApplicationAttemptStateClusterJ.ApplicationAttemptStateDTO applicationAttemptStateDTO = session.newInstance(ApplicationAttemptStateClusterJ.ApplicationAttemptStateDTO.class);
        
        applicationAttemptStateDTO.setappliactionid(hop.getApplicationid());
        applicationAttemptStateDTO.setapplicationattemptid(hop.getApplicationattemptid());
        applicationAttemptStateDTO.setapplicationattemptstate(hop.getApplicationattemptstate());
        
        return applicationAttemptStateDTO;
    }
}
