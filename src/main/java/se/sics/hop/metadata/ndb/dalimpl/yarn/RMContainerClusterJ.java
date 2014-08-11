package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
        @Column(name = CONTAINERID_ID)
        String getcontaineridid();

        void setcontaineridid(String containeridid);

        @Column(name = APPLICATIONATTEMPTID_ID)
        int getappattemptidid();

        void setappattemptidid(int appattemptidid);

        @Column(name = NODEID_ID)
        int getnodeidid();

        void setnodeidid(int nodeidid);

        @Column(name = CONTAINER_ID)
        int getcontainerid();

        void setcontainerid(int containerid);

        @Column(name = RMCONTEXT_ID)
        int getrmcontextid();

        void setrmcontextid(int rmcontextid);

        @Column(name = USER)
        String getuser();

        void setuser(String user);

        @Column(name = RESERVED_RESOURCE_ID)
        int getreservedresourceid();

        void setreservedresourceid(int reservedresourceid);

        @Column(name = RESERVED_NODEID_ID)
        int getreservednodeid();

        void setreservednodeid(int reservednodeid);

        @Column(name = RESERVED_PRIORITY_ID)
        int getreservedpriorityid();

        void setreservedpriorityid(int reservedpriorityid);

        @Column(name = STARTTIME)
        long getstarttime();

        void setstarttime(long starttime);

        @Column(name = FINISHTIME)
        long getfinishtime();

        void setfinishtime(long finishtime);

        @Column(name = CONTAINERSTATUS_ID)
        int getcontainerstatusid();

        void setcontainerstatusid(int containerstatusid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopRMContainer findById(String id) throws StorageException {
        Session session = connector.obtainSession();

        RMContainerDTO rMContainerDTO = null;
        if (session != null) {
            rMContainerDTO = session.find(RMContainerDTO.class, id);
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
                List<RMContainerDTO> toRemove = new ArrayList<RMContainerDTO>(removed.size());
                for (HopRMContainer hop : removed) {
                    toRemove.add(session.newInstance(RMContainerClusterJ.RMContainerDTO.class, hop.getContainerIdID()));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<RMContainerDTO> toModify = new ArrayList<RMContainerDTO>(modified.size());
                for (HopRMContainer hop : modified) {
                    toModify.add(createPersistable(hop, session));
                }
                session.savePersistentAll(toModify);
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
        return new HopRMContainer(
                rMContainerDTO.getcontaineridid(),
                rMContainerDTO.getappattemptidid(),
                rMContainerDTO.getnodeidid(),
                rMContainerDTO.getcontainerid(),
                rMContainerDTO.getrmcontextid(),
                rMContainerDTO.getuser(),
                rMContainerDTO.getreservedresourceid(),
                rMContainerDTO.getreservednodeid(),
                rMContainerDTO.getreservedpriorityid(),
                rMContainerDTO.getstarttime(),
                rMContainerDTO.getfinishtime(),
                rMContainerDTO.getcontainerstatusid());
    }

    private RMContainerDTO createPersistable(HopRMContainer hop, Session session) {
        RMContainerClusterJ.RMContainerDTO rMContainerDTO = session.newInstance(RMContainerClusterJ.RMContainerDTO.class);

        rMContainerDTO.setcontaineridid(hop.getContainerIdID());
        rMContainerDTO.setappattemptidid(hop.getApplicationAttemptIdID());
        rMContainerDTO.setnodeidid(hop.getNodeIdID());
        rMContainerDTO.setcontainerid(hop.getContainerID());
        rMContainerDTO.setrmcontextid(hop.getRmcontextID());
        rMContainerDTO.setuser(hop.getUser());
        rMContainerDTO.setreservedresourceid(hop.getReservedResourceID());
        rMContainerDTO.setreservednodeid(hop.getReservedNodeIdID());
        rMContainerDTO.setreservedpriorityid(hop.getReservedPriorityID());
        rMContainerDTO.setstarttime(hop.getStarttime());
        rMContainerDTO.setfinishtime(hop.getFinishtime());
        rMContainerDTO.setcontainerstatusid(hop.getContainerstatus_id());

        return rMContainerDTO;
    }
}
