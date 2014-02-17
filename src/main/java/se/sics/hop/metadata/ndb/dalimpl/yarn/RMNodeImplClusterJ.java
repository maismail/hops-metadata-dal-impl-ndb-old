package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;
import java.util.List;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopRMNodeImpl;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.RMNodeImplDataAccess;
import se.sics.hop.metadata.yarn.tabledef.RMNodeImplTableDef;
import static se.sics.hop.metadata.yarn.tabledef.RMNodeImplTableDef.COMMAND_PORT;

/**
 * Implements connection of RMNodeImpl to NDB.
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class RMNodeImplClusterJ implements RMNodeImplTableDef, RMNodeImplDataAccess<HopRMNodeImpl> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface RMNodeImplDTO {

        @PrimaryKey
        @Column(name = HOST_NAME)
        String getHostname();

        void setHostname(String hostName);

        @PrimaryKey
        @Column(name = COMMAND_PORT)
        int getCommandport();

        void setCommandport(int commandport);

        @Column(name = HTTP_PORT)
        int getHttpport();

        void setHttpport(int httpport);

        @Column(name = NODE_ADDRESS)
        String getNodeaddress();

        void setNodeaddress(String nodeAddress);

        @Column(name = HTTP_ADDRESS)
        String getHttpaddress();

        void setHttpaddress(String httpAddress);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopRMNodeImpl findByHostNameCommandPort(String hostName, int commandPort) throws StorageException {
        Session session = connector.obtainSession();
        Object[] objarr = new Object[2];
        objarr[0] = hostName;
        objarr[1] = commandPort;
        RMNodeImplDTO rmnodeDTO = session.find(RMNodeImplDTO.class, objarr);
        if (rmnodeDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }
        return createHopRMNodeImpl(rmnodeDTO);
    }

    @Override
    public HopRMNodeImpl findByHostName(String hostName) throws StorageException {
        Session session = connector.obtainSession();
        System.out.println("RMNodeImplClusterJ :: findByHostName-" + hostName + " :: session=" + session.toString());
        RMNodeImplDTO rmnodeDTO = session.find(RMNodeImplDTO.class, hostName);
        if (rmnodeDTO == null) {
            throw new StorageException("Error while retrieving row");
        }
        return createHopRMNodeImpl(rmnodeDTO);
    }

    @Override
    public List<HopRMNodeImpl> findByNodeAddress(String nodeAddress) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void prepare(Collection<HopRMNodeImpl> modified, Collection<HopRMNodeImpl> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopRMNodeImpl rm : removed) {
                    //Object[] objarr = new Object[2];
                    //objarr[0] = attr.getHostName();
                    RMNodeImplDTO persistable = session.newInstance(RMNodeImplDTO.class, rm.getHostName());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopRMNodeImpl rm : modified) {
                    RMNodeImplDTO persistable = createPersistable(rm, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createRMNode(HopRMNodeImpl rmNode) throws StorageException {
        Session session = connector.obtainSession();
        createPersistable(rmNode, session);
    }

    private RMNodeImplDTO createPersistable(HopRMNodeImpl hopRMNode, Session session) {
        RMNodeImplDTO rmDTO = session.newInstance(RMNodeImplDTO.class);
        rmDTO.setCommandport(hopRMNode.getCommandPort());
        rmDTO.setHostname(hopRMNode.getHostName());
        rmDTO.setHttpaddress(hopRMNode.getHttpAddress());
        rmDTO.setHttpport(hopRMNode.getHttpPort());
        rmDTO.setNodeaddress(hopRMNode.getNodeAddress());
        session.savePersistent(rmDTO);
        return rmDTO;
    }

    private HopRMNodeImpl createHopRMNodeImpl(RMNodeImplDTO rmDTO) {
        return new HopRMNodeImpl(rmDTO.getHostname(), rmDTO.getCommandport(), rmDTO.getHttpport(), rmDTO.getHttpaddress(), rmDTO.getNodeaddress());
    }
}
