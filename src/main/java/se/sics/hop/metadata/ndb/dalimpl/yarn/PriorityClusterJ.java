package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopPriority;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.PriorityDataAccess;
import se.sics.hop.metadata.yarn.tabledef.PriorityTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class PriorityClusterJ implements PriorityTableDef, PriorityDataAccess<HopPriority> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface PriorityDTO {

        @PrimaryKey
        @Column(name = ID)
        int getid();

        void setid(int id);

        @Column(name = PRIORITY)
        int getpriorityid();

        void setpriorityid(int priorityid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopPriority findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        PriorityDTO priorityDTO = null;
        if (session != null) {
            priorityDTO = session.find(PriorityDTO.class, id);
        }
        if (priorityDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopPriority(priorityDTO);
    }

    @Override
    public void prepare(Collection<HopPriority> modified, Collection<HopPriority> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                List<PriorityDTO> toRemove = new ArrayList<PriorityDTO>();
                for (HopPriority pr : removed) {
                    toRemove.add(session.newInstance(PriorityDTO.class, pr.getId()));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<PriorityDTO> toModify = new ArrayList<PriorityDTO>();
                for (HopPriority pr : modified) {
                    toModify.add(createPersistable(pr, session));
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException("Error while rmnode table:" + e.getMessage());
        }
    }

    @Override
    public void createPriority(HopPriority priority) throws StorageException {
        Session session = connector.obtainSession();
        session.savePersistent(createPersistable(priority, session));
    }

    private HopPriority createHopPriority(PriorityDTO priorityDTO) {
        return new HopPriority(priorityDTO.getid(), priorityDTO.getpriorityid());
    }

    private PriorityDTO createPersistable(HopPriority priority, Session session) {
        PriorityDTO priorityDTO = session.newInstance(PriorityDTO.class);
        //Set values to persist new rmnode
        priorityDTO.setid(priority.getId());
        priorityDTO.setpriorityid(priority.getPriority());
        return priorityDTO;
    }
}
