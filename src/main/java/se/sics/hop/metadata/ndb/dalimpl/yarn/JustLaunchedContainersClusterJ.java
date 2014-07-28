package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
        @Column(name = RMNODEID)
        int getrmnodeid();

        void setrmnodeid(int rmnodeid);

        @PrimaryKey
        @Column(name = CONTAINERID)
        int getcontainerid();

        void setcontainerid(int containerid);

        @Column(name = CONTAINERSTATUSID)
        int getcontainerstatusid();

        void setcontainerstatusid(int containerstatusid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopJustLaunchedContainers findEntry(int rmnode, int containerid) throws StorageException {
        Session session = connector.obtainSession();
        Object[] objarr = new Object[2];
        objarr[0] = rmnode;
        objarr[1] = containerid;
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
                List<JustLaunchedContainersDTO> toRemove = new ArrayList<JustLaunchedContainersDTO>(removed.size());
                for (HopJustLaunchedContainers hopContainerId : removed) {
                    Object[] objarr = new Object[2];
                    objarr[0] = hopContainerId.getRmnodeid();
                    objarr[1] = hopContainerId.getContainerid();
                    toRemove.add(session.newInstance(JustLaunchedContainersDTO.class, objarr));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<JustLaunchedContainersDTO> toModify = new ArrayList<JustLaunchedContainersDTO>(removed.size());
                for (HopJustLaunchedContainers hop : modified) {
                    toModify.add(createPersistable(hop, session));
                }
                session.savePersistentAll(toModify);
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
        return jlcDTO;
    }
}
