package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopApplicationAttemptId;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.ApplicationAttemptIdDataAccess;
import se.sics.hop.metadata.yarn.tabledef.ApplicationAttemptIdTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class ApplicationAttemptIdClusterJ implements ApplicationAttemptIdTableDef, ApplicationAttemptIdDataAccess<HopApplicationAttemptId> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface ApplicationAttemptIdDTO {

        @PrimaryKey

        @Column(name = ATTEMPTID)
        int getattemptid();
        void setattemptid(int attemptid);

        @Column(name = APPID)
        int getappid();

        void setappid(int appid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopApplicationAttemptId findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        ApplicationAttemptIdDTO appAttemptIdDTO = null;
        if (session != null) {
            appAttemptIdDTO = session.find(ApplicationAttemptIdDTO.class, id);
        }
        if (appAttemptIdDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopApplicationAttemptId(appAttemptIdDTO);
    }
    
    @Override
    public HopApplicationAttemptId findByAttemptIdAppId(int attemptId, int appId) throws StorageException {
         ApplicationAttemptIdClusterJ.ApplicationAttemptIdDTO applicationAttemptIdDTO = null;
        try {
            Session session = connector.obtainSession();

            Object[] objarr = new Object[2];
            objarr[0] = attemptId;
            objarr[1] = appId;

            if (session != null) {
                applicationAttemptIdDTO = session.find(ApplicationAttemptIdClusterJ.ApplicationAttemptIdDTO.class, objarr);
            }
            if (applicationAttemptIdDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row:" + attemptId + ":" + appId);
            }
            return createHopApplicationAttemptId(applicationAttemptIdDTO);
        } catch (Exception e) {
            if (e.getMessage().contains("Tuple did not exist")) {
                throw new StorageException("HOP :: Error while retrieving row:" + attemptId + ":" + appId);
            }
        }
        throw new StorageException("HOP :: Error while retrieving row:" + attemptId + ":" + appId);
    }

    @Override
    public void prepare(Collection<HopApplicationAttemptId> modified, Collection<HopApplicationAttemptId> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopApplicationAttemptId hopAppAttemptId : removed) {

                    ApplicationAttemptIdDTO persistable = session.newInstance(ApplicationAttemptIdDTO.class, hopAppAttemptId.getAttemptId());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopApplicationAttemptId hopAppAttemptId : modified) {
                    ApplicationAttemptIdDTO persistable = createPersistable(hopAppAttemptId, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createApplicationAttemptId(HopApplicationAttemptId applicationattemptid) throws StorageException {
        Session session = connector.obtainSession();
        createPersistable(applicationattemptid, session);
    }

    private ApplicationAttemptIdDTO createPersistable(HopApplicationAttemptId hopAppAttemptId, Session session) {
        ApplicationAttemptIdDTO appAttemptIdDTO = session.newInstance(ApplicationAttemptIdDTO.class);
        appAttemptIdDTO.setappid(hopAppAttemptId.getAppid());
        appAttemptIdDTO.setattemptid(hopAppAttemptId.getAttemptId());
        session.savePersistent(appAttemptIdDTO);
        return appAttemptIdDTO;
    }

    private HopApplicationAttemptId createHopApplicationAttemptId(ApplicationAttemptIdDTO appAttemptIdDTO) {
        HopApplicationAttemptId hop = new HopApplicationAttemptId(appAttemptIdDTO.getattemptid(), appAttemptIdDTO.getappid());
        return hop;
    }
}
