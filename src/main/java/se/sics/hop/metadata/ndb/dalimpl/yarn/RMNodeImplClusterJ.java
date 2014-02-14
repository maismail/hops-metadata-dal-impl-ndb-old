package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.List;
import se.sics.hop.metadata.hdfs.entity.yarn.HopRMNodeImpl;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.RMNodeImplDataAccess;
import se.sics.hop.metadata.yarn.tabledef.RMNodeImplTableDef;
import static se.sics.hop.metadata.yarn.tabledef.RMNodeImplTableDef.COMMAND_PORT;
import static se.sics.hop.metadata.yarn.tabledef.RMNodeImplTableDef.NODE_ADDRESS;

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
        String getHostName();

        void setHostName(String hostName);

        @Column(name = COMMAND_PORT)
        int getCommandPort();

        void setCommandPort(int port);

        //@PrimaryKey
        @Column(name = HTTP_PORT)
        int getHttpPort();

        void setHttpPort(int port);

        @Column(name = NODE_ADDRESS)
        String getNodeAddress();

        void setNodeAddress(String nodeAddress);

        @Column(name = HTTP_ADDRESS)
        String getHttpAddress();

        void setHttpAddress(String httpAddress);

    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopRMNodeImpl findByHostNameHttpPort(String hostName, int httpPort) {
        Session session = connector.obtainSession();
        Object[] objarr = new Object[2];
        objarr[0] = hostName;
        objarr[1] = httpPort;
        RMNodeImplDTO rmnodeDTO = session.find(RMNodeImplDTO.class, objarr);
        if(rmnodeDTO == null){
            System.out.println("test:");
        }
        int a = rmnodeDTO.getHttpPort();
        System.out.println("test:"+rmnodeDTO.getHttpPort());
        return createHopRMNodeImpl(rmnodeDTO);

    }
    @Override
    public HopRMNodeImpl findByHostName(String hostName) {
        System.out.println("findByHostName :: hostname="+hostName);
        Session session = connector.obtainSession();
        System.out.println("findByHostName :: session="+session.toString());
        RMNodeImplDTO rmnodeDTO = session.find(RMNodeImplDTO.class, hostName);
        if(rmnodeDTO == null){
            System.out.println("findByHostName :: rmnodeDTO is null");
            return null;
        }
        int a = rmnodeDTO.getHttpPort();
        System.out.println("test:"+rmnodeDTO.getHttpPort());
        return createHopRMNodeImpl(rmnodeDTO);
    }
    @Override
    public List<HopRMNodeImpl> findByNodeAddress(String nodeAddress) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setRMNodeImpl(HopRMNodeImpl hopRMNode) {
        Session session = connector.obtainSession();
        RMNodeImplDTO rmDTO = session.newInstance(RMNodeImplDTO.class);
        rmDTO.setCommandPort(hopRMNode.getCommandPort());
        rmDTO.setHostName(hopRMNode.getHostName());
        rmDTO.setHttpAddress("ixixi");
        rmDTO.setHttpPort(hopRMNode.getHttpPort());
        rmDTO.setNodeAddress(hopRMNode.getNodeAddress());
        session.savePersistent(rmDTO);
    }

    private HopRMNodeImpl createHopRMNodeImpl(RMNodeImplDTO rmDTO) {
        return new HopRMNodeImpl(rmDTO.getHostName(), rmDTO.getCommandPort(), rmDTO.getHttpPort(), rmDTO.getHttpAddress(), rmDTO.getNodeAddress());
    }

}
