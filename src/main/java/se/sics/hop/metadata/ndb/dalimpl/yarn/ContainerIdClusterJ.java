package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
        String getid();
        void setid(String id);
        
        @Column(name = CONTAINERID)
        int getcontid();

        void setcontid(int contid);

        @Column(name = APPLICATIONATTEMPT_ID)
        String getapplicationattemptid();

        void setapplicationattemptid(String applicationattemptid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopContainerId findById(String id) throws StorageException {
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
                List<ContainerIdDTO> toRemove = new ArrayList<ContainerIdDTO>();
                for (HopContainerId hopContainerId : removed) {
                    toRemove.add(session.newInstance(ContainerIdDTO.class, hopContainerId.getId()));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<ContainerIdDTO> toModify = new ArrayList<ContainerIdDTO>();
                for (HopContainerId hopContainerId : modified) {
                    toModify.add(createPersistable(hopContainerId, session));
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createContainerId(HopContainerId containerstatus) throws StorageException {
        Session session = connector.obtainSession();
        session.savePersistent(createPersistable(containerstatus, session));
    }

    private ContainerIdDTO createPersistable(HopContainerId hopContainerId, Session session) {
        ContainerIdDTO containerIdDTO = session.newInstance(ContainerIdDTO.class);
        //Set values to persist new ContainerStatus
        containerIdDTO.setid(hopContainerId.getId());
        containerIdDTO.setcontid(hopContainerId.getContainerId());
        containerIdDTO.setapplicationattemptid(hopContainerId.getApplicationAttemptId());
        return containerIdDTO;
    }

    private HopContainerId createHopContainerId(ContainerIdDTO containerIdDTO) {
        HopContainerId hop = new HopContainerId(containerIdDTO.getid(), containerIdDTO.getcontid(), containerIdDTO.getapplicationattemptid());
        return hop;
    }
}
