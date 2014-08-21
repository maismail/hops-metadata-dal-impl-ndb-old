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
        String getappattemptidid();

        void setappattemptidid(String appattemptidid);

        @Column(name = NODEID_ID)
        String getnodeidid();

        void setnodeidid(String nodeidid);

        @Column(name = USER)
        String getuser();

        void setuser(String user);

        @Column(name = RESERVED_NODEID_ID)
        String getreservednodeid();

        void setreservednodeid(String reservednodeid);

        @Column(name = RESERVED_PRIORITY_ID)
        int getreservedpriorityid();

        void setreservedpriorityid(int reservedpriorityid);

        @Column(name = STARTTIME)
        long getstarttime();

        void setstarttime(long starttime);

        @Column(name = FINISHTIME)
        long getfinishtime();

        void setfinishtime(long finishtime);
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
                rMContainerDTO.getuser(),
                rMContainerDTO.getreservednodeid(),
                rMContainerDTO.getreservedpriorityid(),
                rMContainerDTO.getstarttime(),
                rMContainerDTO.getfinishtime());
    }

    private RMContainerDTO createPersistable(HopRMContainer hop, Session session) {
        RMContainerClusterJ.RMContainerDTO rMContainerDTO = session.newInstance(RMContainerClusterJ.RMContainerDTO.class);

        rMContainerDTO.setcontaineridid(hop.getContainerIdID());
        rMContainerDTO.setappattemptidid(hop.getApplicationAttemptIdID());
        rMContainerDTO.setnodeidid(hop.getNodeIdID());
        rMContainerDTO.setuser(hop.getUser());
        rMContainerDTO.setreservednodeid(hop.getReservedNodeIdID());
        rMContainerDTO.setreservedpriorityid(hop.getReservedPriorityID());
        rMContainerDTO.setstarttime(hop.getStarttime());
        rMContainerDTO.setfinishtime(hop.getFinishtime());

        return rMContainerDTO;
    }
}
