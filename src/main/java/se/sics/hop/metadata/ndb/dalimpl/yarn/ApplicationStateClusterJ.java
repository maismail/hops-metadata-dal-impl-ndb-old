package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopApplicationState;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.ApplicationStateDataAccess;
import se.sics.hop.metadata.yarn.tabledef.ApplicationStateTableDef;

/**
 *
 * @author nickstanogias
 */
public class ApplicationStateClusterJ implements ApplicationStateTableDef, ApplicationStateDataAccess<HopApplicationState> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface ApplicationStateDTO {

        @PrimaryKey
        @Column(name = APPLICATIONID)
        String getapplicationid();

        void setapplicationid(String applicationid);

        @Column(name = APPSTATE)
        byte[] getappstate();

        void setappstate(byte[] appstate);
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopApplicationState findByApplicationId(int id) throws StorageException {
        Session session = connector.obtainSession();

        ApplicationStateClusterJ.ApplicationStateDTO appStateDTO = null;
        if (session != null) {
            appStateDTO = session.find(ApplicationStateClusterJ.ApplicationStateDTO.class, id);
        }
        if (appStateDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopAppState(appStateDTO);
    }

    @Override
    public void prepare(Collection<HopApplicationState> modified, Collection<HopApplicationState> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopApplicationState hop : removed) {
                    ApplicationStateDTO persistable = session.newInstance(ApplicationStateDTO.class, hop.getApplicationid());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopApplicationState hop : modified) {
                    ApplicationStateDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    private HopApplicationState createHopAppState(ApplicationStateDTO appStateDTO) {
        return new HopApplicationState(appStateDTO.getapplicationid(),
                appStateDTO.getappstate());
    }

    private ApplicationStateDTO createPersistable(HopApplicationState hop, Session session) {
        ApplicationStateDTO appStateDTO = session.newInstance(ApplicationStateClusterJ.ApplicationStateDTO.class);
        appStateDTO.setapplicationid(hop.getApplicationid());
        appStateDTO.setappstate(hop.getAppstate());

        return appStateDTO;
    }
}
