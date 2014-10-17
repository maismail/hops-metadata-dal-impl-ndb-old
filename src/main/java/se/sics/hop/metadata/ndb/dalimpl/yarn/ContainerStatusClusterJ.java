package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopContainerStatus;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.ContainerStatusDataAccess;
import se.sics.hop.metadata.yarn.tabledef.ContainerStatusTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class ContainerStatusClusterJ implements ContainerStatusTableDef, ContainerStatusDataAccess<HopContainerStatus> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface ContainerStatusDTO {

        @PrimaryKey
        @Column(name = CONTAINERID)
        String getcontainerid();

        void setcontainerid(String containerid);

        @Column(name = STATE)
        String getstate();

        void setstate(String state);

        @Column(name = DIAGNOSTICS)
        String getdiagnostics();

        void setdiagnostics(String diagnostics);

        @Column(name = EXIT_STATUS)
        int getexitstatus();

        void setexitstatus(int exitstatus);
        
        @Column(name= RMNODEID)
        String getrmnodeid();
        
        void setrmnodeid(String rmnodeid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopContainerStatus findById(String id) throws StorageException {
        Session session = connector.obtainSession();

        ContainerStatusDTO uciDTO = null;
        if (session != null) {
            uciDTO = session.find(ContainerStatusDTO.class, id);
        }
        if (uciDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopContainerStatus(uciDTO);
    }

    @Override
    public void prepare(Collection<HopContainerStatus> modified, Collection<HopContainerStatus> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                List<ContainerStatusDTO> toRemove = new ArrayList<ContainerStatusDTO>();
                for (HopContainerStatus hopUCI : removed) {
                    toRemove.add(session.newInstance(ContainerStatusDTO.class, hopUCI.getContainerid()));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<ContainerStatusDTO> toModify = new ArrayList<ContainerStatusDTO>();
                for (HopContainerStatus hopUCI : modified) {
                    toModify.add(createPersistable(hopUCI, session));
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createContainerStatus(HopContainerStatus containerstatus) throws StorageException {
        Session session = connector.obtainSession();
        session.savePersistent(createPersistable(containerstatus, session));
    }

    private ContainerStatusDTO createPersistable(HopContainerStatus hopCS, Session session) {
        ContainerStatusDTO csDTO = session.newInstance(ContainerStatusDTO.class);
        //Set values to persist new ContainerStatus
        csDTO.setcontainerid(hopCS.getContainerid());
        csDTO.setstate(hopCS.getState());
        csDTO.setdiagnostics(hopCS.getDiagnostics());
        csDTO.setexitstatus(hopCS.getExitstatus());
        csDTO.setrmnodeid(hopCS.getRMNodeId());
        return csDTO;
    }

    private HopContainerStatus createHopContainerStatus(ContainerStatusDTO csDTO) {
        HopContainerStatus hop = new HopContainerStatus(csDTO.getcontainerid(), csDTO.getstate(), csDTO.getdiagnostics(), csDTO.getexitstatus(),
        csDTO.getrmnodeid());
        return hop;
    }

    private List<HopContainerStatus> createHopContainerStatusList(List<ContainerStatusDTO> listDTO) {
        List<HopContainerStatus> hopList = new ArrayList<HopContainerStatus>();
        for (ContainerStatusDTO persistable : listDTO) {
            hopList.add(createHopContainerStatus(persistable));
        }
        return hopList;
    }
}
