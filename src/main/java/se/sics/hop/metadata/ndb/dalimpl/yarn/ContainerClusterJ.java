package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopContainer;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.ContainerDataAccess;
import se.sics.hop.metadata.yarn.tabledef.ContainerTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class ContainerClusterJ implements ContainerTableDef, ContainerDataAccess<HopContainer> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface ContainerDTO {

        @PrimaryKey
        @Column(name = CONTAINERID_ID)
        String getcontaineridid();
        void setcontaineridid(String containeridid);

        @Column(name = NODEID_ID)
        int getnodeidid();

        void setnodeidid(int nodeidid);

        @Column(name = NODEHTTPADDRESS)
        String getnodehttpaddress();
        void setnodehttpaddress(String nodehttpaddress);
        
        @Column(name = PRIORITY_ID)
        int getpriorityid();

        void setpriorityid(int priorityid);

        @Column(name = RESOURCE_ID)
        int getresourceid();

        void setresourceid(int resourceid);

        @Column(name = TOKEN_ID)
        int gettokenid();

        void settokenid(int tokenid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopContainer findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        ContainerDTO containerDTO = null;
        if (session != null) {
            containerDTO = session.find(ContainerDTO.class, id);
        }
        if (containerDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopContainer(containerDTO);
    }

    @Override
    public void prepare(Collection<HopContainer> modified, Collection<HopContainer> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopContainer hopContainer : removed) {

                    ContainerDTO persistable = session.newInstance(ContainerDTO.class, hopContainer.getContainerIdID());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopContainer hopContainer : modified) {
                    ContainerDTO persistable = createPersistable(hopContainer, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createContainer(HopContainer container) throws StorageException {
        Session session = connector.obtainSession();
        createPersistable(container, session);
    }

    private HopContainer createHopContainer(ContainerDTO containerDTO) {
        HopContainer hop = new HopContainer(containerDTO.getcontaineridid(),
                containerDTO.getnodeidid(),
                containerDTO.getnodehttpaddress(),
                containerDTO.getresourceid(),
                containerDTO.getpriorityid(),
                containerDTO.gettokenid());
        return hop;
    }

    private ContainerDTO createPersistable(HopContainer hopContainer, Session session) {
        ContainerDTO containerDTO = session.newInstance(ContainerDTO.class);
        containerDTO.setcontaineridid(hopContainer.getContainerIdID());
        containerDTO.setnodeidid(hopContainer.getNodeIdID());
        containerDTO.setnodehttpaddress(hopContainer.getNodehttpaddress());
        containerDTO.setpriorityid(hopContainer.getPriorityID());
        containerDTO.setresourceid(hopContainer.getResourceID());
        containerDTO.settokenid(hopContainer.getTokenID());
        session.savePersistent(containerDTO);
        return containerDTO;
    }
}
