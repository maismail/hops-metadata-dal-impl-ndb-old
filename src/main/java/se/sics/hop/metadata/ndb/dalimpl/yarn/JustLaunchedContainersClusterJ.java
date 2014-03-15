package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopJustLaunchedContainers;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.JustLaunchedContainersDataAccess;
import se.sics.hop.metadata.yarn.tabledef.JustLaunchedContainersTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class JustLaunchedContainersClusterJ implements JustLaunchedContainersTableDef, JustLaunchedContainersDataAccess<HopJustLaunchedContainers> {
    
    @PersistenceCapable(table = TABLE_NAME)
    public interface JustLaunchedContainersDTO {
        
        @PrimaryKey
        @Column(name = CONTAINERID)
        int getcontainerid();
        
        void setcontainerid(int containerid);
        
        @Column(name = CONTAINERSTATUSID)
        int getcontainerstatusid();
        
        void setcontainerstatusid(int containerstatusid);
        
        @Column(name = RMNODEID)
        int getrmnodeid();

        void setrmnodeid(int rmnodeid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public HopJustLaunchedContainers findEntry(int key, int rmnode) throws StorageException {
        Session session = connector.obtainSession();
        Object[] objarr = new Object[2];
        objarr[0] = key;
        objarr[1] = rmnode;
        JustLaunchedContainersDTO container = null;
        if (session != null) {
            container = session.find(JustLaunchedContainersDTO.class, objarr);
        }
        if (container == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }
        
        return createJustLaunchedContainers(container);
    }
    
    @Override
    public void prepare(Collection<HopJustLaunchedContainers> modified, Collection<HopJustLaunchedContainers> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopJustLaunchedContainers hopContainerId : removed) {
                    Object[] objarr = new Object[2];
                    objarr[0] = hopContainerId.getRmnodeid();
                    objarr[1] = hopContainerId.getContainerid();
                    JustLaunchedContainersDTO persistable = session.newInstance(JustLaunchedContainersDTO.class, objarr);
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopJustLaunchedContainers id : modified) {
                    JustLaunchedContainersDTO persistable = createPersistable(id, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    @Override
    public void createJustLaunchedContainerEntry(HopJustLaunchedContainers entry) throws StorageException {
        Session session = connector.obtainSession();
        createPersistable(entry, session);
    }
    
    private HopJustLaunchedContainers createJustLaunchedContainers(JustLaunchedContainersDTO container) {
        HopJustLaunchedContainers hop = new HopJustLaunchedContainers(container.getcontainerid(), container.getcontainerstatusid(), container.getrmnodeid());
        return hop;
    }

    /**
     * Persist new map entry.
     *
     * @param entry
     * @param session
     * @return
     */
    private JustLaunchedContainersDTO createPersistable(HopJustLaunchedContainers entry, Session session) {
        JustLaunchedContainersDTO jlcDTO = session.newInstance(JustLaunchedContainersDTO.class);
        jlcDTO.setcontainerid(entry.getContainerid());
        jlcDTO.setcontainerstatusid(entry.getContainerstatusid());
        jlcDTO.setrmnodeid(entry.getRmnodeid());
        session.savePersistent(jlcDTO);
        return jlcDTO;
    }
}
