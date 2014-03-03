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
        @Column(name = ID)
        int getid();

        void setid(int id);

        @Column(name = APPLICATION_ID)
        int getapplicationid();

        void setapplicationid(int applicationid);
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
    public void prepare(Collection<HopApplicationAttemptId> modified, Collection<HopApplicationAttemptId> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopApplicationAttemptId hopAppAttemptId : removed) {

                    ApplicationAttemptIdDTO persistable = session.newInstance(ApplicationAttemptIdDTO.class, hopAppAttemptId.getId());
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
        appAttemptIdDTO.setid(hopAppAttemptId.getId());
        appAttemptIdDTO.setapplicationid(hopAppAttemptId.getApplicationid());
        session.savePersistent(appAttemptIdDTO);
        return appAttemptIdDTO;
    }

    private HopApplicationAttemptId createHopApplicationAttemptId(ApplicationAttemptIdDTO appAttemptIdDTO) {
        HopApplicationAttemptId hop = new HopApplicationAttemptId(appAttemptIdDTO.getid(), appAttemptIdDTO.getapplicationid());
        return hop;
    }
}
