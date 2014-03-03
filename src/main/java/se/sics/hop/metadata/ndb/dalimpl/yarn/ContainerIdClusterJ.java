package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopContainerId;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.ContainerIdDataAccess;
import se.sics.hop.metadata.yarn.tabledef.ContainerIdTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class ContainerIdClusterJ implements ContainerIdTableDef, ContainerIdDataAccess<HopContainerId> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface ContainerIdDTO {

        @PrimaryKey
        @Column(name = ID)
        int getid();

        void setid(int id);

        @Column(name = APPLICATIONATTEMPT_ID)
        int getapplicationattemptid();

        void setapplicationattemptid(int applicationattemptid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopContainerId findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        ContainerIdDTO containerIdDTO = null;
        if (session != null) {
            containerIdDTO = session.find(ContainerIdDTO.class, id);
        }
        if (containerIdDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopContainerId(containerIdDTO);
    }

    @Override
    public void prepare(Collection<HopContainerId> modified, Collection<HopContainerId> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopContainerId hopContainerId : removed) {

                    ContainerIdDTO persistable = session.newInstance(ContainerIdDTO.class, hopContainerId.getId());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopContainerId hopContainerId : modified) {
                    ContainerIdDTO persistable = createPersistable(hopContainerId, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createContainerStatus(HopContainerId containerstatus) throws StorageException {
        Session session = connector.obtainSession();
        createPersistable(containerstatus, session);
    }

    private ContainerIdDTO createPersistable(HopContainerId hopContainerId, Session session) {
        ContainerIdDTO containerIdDTO = session.newInstance(ContainerIdDTO.class);
        //Set values to persist new ContainerStatus
        containerIdDTO.setid(hopContainerId.getId());
        containerIdDTO.setapplicationattemptid(hopContainerId.getApplicationAttemptId());
        session.savePersistent(containerIdDTO);
        return containerIdDTO;
    }

    private HopContainerId createHopContainerId(ContainerIdDTO containerIdDTO) {
        HopContainerId hop = new HopContainerId(containerIdDTO.getid(), containerIdDTO.getapplicationattemptid());
        return hop;
    }
}
