package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopRMNode;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.RMNodeDataAccess;
import se.sics.hop.metadata.yarn.tabledef.RMNodeTableDef;
import static se.sics.hop.metadata.yarn.tabledef.RMNodeTableDef.COMMAND_PORT;
import static se.sics.hop.metadata.yarn.tabledef.RMNodeTableDef.RESOURCE_ID;

/**
 * Implements connection of RMNodeImpl to NDB.
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class RMNodeClusterJ implements RMNodeTableDef, RMNodeDataAccess<HopRMNode> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface RMNodeDTO {

        @PrimaryKey
        @Column(name = NODEID)
        int getNodeid();

        void setNodeid(int nodeid);

        @Column(name = HOST_NAME)
        String getHostname();

        void setHostname(String hostName);

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

        @Column(name = NEXT_HEARTBEAT)
        boolean getNextheartbeat();

        void setNextheartbeat(boolean nexthearbeat);

        @Column(name = RESOURCE_ID)
        int getResourceid();

        void setResourceid(int resourceid);

        @Column(name = NODEBASEID)
        int getNodebaseid();

        void setNodebaseid(int nodebaseid);

        @Column(name = HEALTH_REPORT)
        String getHealthreport();

        void setHealthreport(String healthreport);

        @Column(name = RMCONTEXT_ID)
        int getRMContextid();

        void setRMContextid(int rmcontextid);

        @Column(name = LAST_HEALTH_REPORT_TIME)
        long getLasthealthreporttime();

        void setLasthealthreporttime(long lasthealthreporttime);

        @Column(name = CURRENT_STATE)
        String getcurrentstate();

        void setcurrentstate(String currentstate);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopRMNode findByNodeId(int nodeid) throws StorageException {
        Session session = connector.obtainSession();
        RMNodeDTO rmnodeDTO = session.find(RMNodeDTO.class, nodeid);
        if (rmnodeDTO == null) {
            throw new StorageException("Error while retrieving row:" + nodeid);
        }

        return createHopRMNode(rmnodeDTO);
    }

    @Override
    public HopRMNode findByHostNameCommandPort(String hostName, int commandPort) throws StorageException {
        Session session = connector.obtainSession();
        Object[] objarr = new Object[2];
        objarr[0] = hostName;
        objarr[1] = commandPort;
        RMNodeDTO rmnodeDTO = session.find(RMNodeDTO.class, objarr);
        if (rmnodeDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row:" + hostName + ":" + commandPort);
        }
        return createHopRMNode(rmnodeDTO);
    }

    @Override
    public HopRMNode findByHostName(String hostName) throws StorageException {
        Session session = connector.obtainSession();
        RMNodeDTO rmnodeDTO = session.find(RMNodeDTO.class, hostName);
        if (rmnodeDTO == null) {
            throw new StorageException("Error while retrieving row:" + hostName);
        }
        return createHopRMNode(rmnodeDTO);
    }

    @Override
    public List<HopRMNode> findByNodeAddress(String nodeAddress) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void deleteAll(int startId, int endId) throws StorageException {
        Session session = connector.obtainSession();
        for (int i = startId; i < endId; i++) {
            RMNodeDTO rmnodeDTO = session.find(RMNodeDTO.class, i);
            session.deletePersistent(rmnodeDTO);
        }
        //session.deletePersistentAll(RMNodeDTO.class);
    }

    @Override
    public void prepare(Collection<HopRMNode> modified, Collection<HopRMNode> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                List<RMNodeDTO> toRemove = new ArrayList<RMNodeDTO>();
                for (HopRMNode rm : removed) {
                    toRemove.add(session.newInstance(RMNodeDTO.class, rm.getNodeId()));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<RMNodeDTO> toModify = new ArrayList<RMNodeDTO>();
                for (HopRMNode rm : modified) {
                    toModify.add(createPersistable(rm, session));
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException("Error while modifying invokerequests, error:" + e.getMessage());
        }
    }

    @Override
    public void createRMNode(HopRMNode rmNode) throws StorageException {
        Session session = connector.obtainSession();
        
            session.savePersistent(createPersistable(rmNode, session));
        
    }

    private RMNodeDTO createPersistable(HopRMNode hopRMNode, Session session) {
        RMNodeDTO rmDTO = session.newInstance(RMNodeDTO.class);

        //Set values to persist new rmnode
        rmDTO.setNodeid(hopRMNode.getNodeId());
        rmDTO.setHostname(hopRMNode.getHostName());
        rmDTO.setCommandport(hopRMNode.getCommandPort());
        rmDTO.setHttpport(hopRMNode.getHttpPort());
        rmDTO.setNodeaddress(hopRMNode.getNodeAddress());
        rmDTO.setHttpaddress(hopRMNode.getHttpAddress());
        rmDTO.setNextheartbeat(hopRMNode.isNextHeartbeat());
        if (hopRMNode.getResourceId() > 0) {
            rmDTO.setResourceid(hopRMNode.getResourceId());
        }
        if (hopRMNode.getNodebaseId() > 0) {
            rmDTO.setNodebaseid(hopRMNode.getNodebaseId());
        }
        rmDTO.setHealthreport(hopRMNode.getHealthReport());
        rmDTO.setRMContextid(hopRMNode.getRmcontextId());
        rmDTO.setLasthealthreporttime(hopRMNode.getLastHealthReportTime());
        rmDTO.setcurrentstate(hopRMNode.getCurrentState());
        ////////////////////////////////////
        //session.savePersistent(rmDTO);
        return rmDTO;
    }

    /**
     * Transforms a DTO to Hop object.
     *
     * @param rmDTO
     * @return HopRMNode
     */
    private HopRMNode createHopRMNode(RMNodeDTO rmDTO) {
        return new HopRMNode(rmDTO.getNodeid(), rmDTO.getHostname(), rmDTO.getCommandport(), rmDTO.getHttpport(), rmDTO.getNodeaddress(), rmDTO.getHttpaddress(), rmDTO.getNextheartbeat(), rmDTO.getResourceid(), rmDTO.getNodebaseid(), rmDTO.getHealthreport(), rmDTO.getRMContextid(), rmDTO.getLasthealthreporttime(), rmDTO.getcurrentstate());
    }
}
