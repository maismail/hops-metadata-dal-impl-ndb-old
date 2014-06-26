package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopRMContainer;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.RMContainerDataAccess;
import se.sics.hop.metadata.yarn.tabledef.RMContainerTableDef;
import static se.sics.hop.metadata.yarn.tabledef.RMContainerTableDef.RESERVED_RESOURCE_ID;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class RMContainerClusterJ implements RMContainerTableDef, RMContainerDataAccess<HopRMContainer> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface RMContainerDTO {

        @PrimaryKey
        @Column(name = ID)
        int getid();

        void setid(int id);

        @Column(name = CONTAINERID_ID)
        int getcontaineridid();

        void setcontaineridid(int containeridid);

        @Column(name = APPLICATIONATTEMPTID_ID)
        int getappattemptidid();

        void setappattemptidid(int appattemptidid);

        @Column(name = NODEID_ID)
        int getnodeidid();

        void setnodeidid(int nodeidid);

        @Column(name = CONTAINER_ID)
        int getcontainerid();

        void setcontainerid(int containerid);

        @Column(name = RESERVED_RESOURCE_ID)
        int getreservedresourceid();

        void setreservedresourceid(int reservedresourceid);
        
        @Column(name = RESERVED_NODEID_ID)
        int getreservednodeid();

        void setreservednodeid(int reservednodeid);

        @Column(name = RESERVED_PRIORITY_ID)
        int getreservedpriorityid();

        void setreservedpriorityid(int reservedpriorityid);

        @Column(name = STATE)
        String getstate();

        void setstate(String state);

        @Column(name = NEWLY_ALLOCATED)
        int getnewlyallocated();

        void setnewlyallocated(int newlyallocated);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopRMContainer findById(int id) throws StorageException {
        Session session = connector.obtainSession();
        
        RMContainerClusterJ.RMContainerDTO rMContainerDTO = null;
        if (session != null) {
            rMContainerDTO = session.find(RMContainerClusterJ.RMContainerDTO.class, id);
        }
        if (rMContainerDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopRMContainer(rMContainerDTO);
    }

    @Override
    public void prepare(Collection<HopRMContainer> modified, Collection<HopRMContainer> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopRMContainer hop : removed) {
                    RMContainerClusterJ.RMContainerDTO persistable = session.newInstance(RMContainerClusterJ.RMContainerDTO.class, hop.getId());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopRMContainer hop : modified) {
                    RMContainerClusterJ.RMContainerDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createRMContainer(HopRMContainer rmcontainer) throws StorageException {
        Session session = connector.obtainSession();
        session.savePersistent(createPersistable(rmcontainer, session));
    }
    
    private HopRMContainer createHopRMContainer(RMContainerDTO rMContainerDTO) {
        return new HopRMContainer(rMContainerDTO.getid(),
                                rMContainerDTO.getcontaineridid(),
                                rMContainerDTO.getappattemptidid(),
                                rMContainerDTO.getnodeidid(),
                                rMContainerDTO.getcontainerid(),
                                rMContainerDTO.getreservedresourceid(),
                                rMContainerDTO.getreservednodeid(),
                                rMContainerDTO.getreservedpriorityid(),
                                rMContainerDTO.getstate(),
                                rMContainerDTO.getnewlyallocated());
    }

    private RMContainerDTO createPersistable(HopRMContainer hop, Session session) {
        RMContainerClusterJ.RMContainerDTO rMContainerDTO = session.newInstance(RMContainerClusterJ.RMContainerDTO.class);
        
        rMContainerDTO.setappattemptidid(hop.getApplicationAttemptIdID());
        rMContainerDTO.setcontainerid(hop.getContainerID());
        rMContainerDTO.setcontaineridid(hop.getContainerIdID());
        rMContainerDTO.setid(hop.getId());
        rMContainerDTO.setnewlyallocated(hop.getNewlyAllocated());
        rMContainerDTO.setnodeidid(hop.getNodeIdID());
        rMContainerDTO.setreservednodeid(hop.getReservedNodeIdID());
        rMContainerDTO.setreservedpriorityid(hop.getReservedPriorityID());
        rMContainerDTO.setreservedresourceid(hop.getReservedResourceID());
        rMContainerDTO.setstate(hop.getState());
        
        return rMContainerDTO;
    }
}
