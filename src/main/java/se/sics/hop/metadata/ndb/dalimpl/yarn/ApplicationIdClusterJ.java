package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopApplicationId;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.ApplicationIdDataAccess;
import se.sics.hop.metadata.yarn.tabledef.ApplicationIdTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class ApplicationIdClusterJ implements ApplicationIdTableDef, ApplicationIdDataAccess<HopApplicationId> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface ApplicationIdDTO {

        @PrimaryKey
        @Column(name = ID)
        int getid();

        void setid(int id);

        @Column(name = CLUSTER_TIMESTAMP)
        long getclustertimestamp();

        void setclustertimestamp(long clustertimestamp);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopApplicationId findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        ApplicationIdDTO applicationIdDTO = null;
        if (session != null) {
            applicationIdDTO = session.find(ApplicationIdDTO.class, id);
        }
        if (applicationIdDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopApplicationId(applicationIdDTO);
    }

    @Override
    public void prepare(Collection<HopApplicationId> modified, Collection<HopApplicationId> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopApplicationId hopApplicationId : removed) {

                    ApplicationIdDTO persistable = session.newInstance(ApplicationIdDTO.class, hopApplicationId.getId());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopApplicationId hopAppAttemptId : modified) {
                    ApplicationIdDTO persistable = createPersistable(hopAppAttemptId, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createApplicationId(HopApplicationId applicationattemptid) throws StorageException {
        Session session = connector.obtainSession();
        createPersistable(applicationattemptid, session);
    }

    private ApplicationIdDTO createPersistable(HopApplicationId hopApplicationId, Session session) {
        ApplicationIdDTO applicationIdDTO = session.newInstance(ApplicationIdDTO.class);
        applicationIdDTO.setid(hopApplicationId.getId());
        applicationIdDTO.setclustertimestamp(hopApplicationId.getClustertimestamp());
        session.savePersistent(applicationIdDTO);
        return applicationIdDTO;
    }

    private HopApplicationId createHopApplicationId(ApplicationIdDTO applicationIdDTO) {
        HopApplicationId hop = new HopApplicationId(applicationIdDTO.getid(), applicationIdDTO.getclustertimestamp());
        return hop;
    }
}
