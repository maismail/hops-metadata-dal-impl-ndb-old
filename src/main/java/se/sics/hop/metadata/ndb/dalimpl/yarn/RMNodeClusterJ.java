package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopRMNode;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.NdbBoolean;
import se.sics.hop.metadata.ndb.wrapper.HopsPredicate;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.RMNodeDataAccess;
import se.sics.hop.metadata.yarn.tabledef.RMNodeTableDef;

/**
 * Implements connection of RMNodeImpl to NDB.
 */
public class RMNodeClusterJ implements RMNodeTableDef, RMNodeDataAccess<HopRMNode> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface RMNodeDTO {

        @PrimaryKey
        @Column(name = NODEID)
        String getNodeid();

        void setNodeid(String nodeid);

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
        byte getNextheartbeat();

        void setNextheartbeat(byte nexthearbeat);

        @Column(name = HEALTH_REPORT)
        String getHealthreport();

        void setHealthreport(String healthreport);

        @Column(name = LAST_HEALTH_REPORT_TIME)
        long getLasthealthreporttime();

        void setLasthealthreporttime(long lasthealthreporttime);

        @Column(name = CURRENT_STATE)
        String getcurrentstate();

        void setcurrentstate(String currentstate);

        @Column(name = OVERCOMMIT_TIMEOUT)
        int getovercommittimeout();

        void setovercommittimeout(int overcommittimeout);

        @Column(name = NODEMANAGER_VERSION)
        String getnodemanagerversion();

        void setnodemanagerversion(String nodemanagerversion);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopRMNode findByNodeId(String nodeid) throws StorageException {
        HopsSession session = connector.obtainSession();
        RMNodeDTO rmnodeDTO = session.find(RMNodeDTO.class, nodeid);
        if (rmnodeDTO == null) {
            throw new StorageException("Error while retrieving row:" + nodeid);
        }
        return createHopRMNode(rmnodeDTO);
    }

    @Override
    public HopRMNode findByHostNameCommandPort(String hostName, int commandPort) throws StorageException {
        HopsSession session = connector.obtainSession();
        HopsQueryBuilder qb = session.getQueryBuilder();
        HopsQueryDomainType<RMNodeDTO> dobj = qb.createQueryDefinition(RMNodeDTO.class);
        HopsPredicate pred1 = dobj.get("hostname").equal(dobj.param("hostname"));
        HopsPredicate pred2 = dobj.get("commandport").equal(dobj.param("commandport"));
        pred1 = pred1.and(pred2);
        dobj.where(pred1);
        HopsQuery<RMNodeDTO> query = session.createQuery(dobj);
        query.setParameter("hostname", hostName);
        query.setParameter("commandport", commandPort);
        List<RMNodeDTO> results = query.getResultList();
        if (results != null && !results.isEmpty()) {
            return createHopRMNode(results.get(0));
        } else {
            throw new StorageException("HOP :: RMNode with host:" + hostName + ", cmport:" + commandPort + " was not found");
        }
    }

    @Override
    public HopRMNode findByHostName(String hostName) throws StorageException {
        HopsSession session = connector.obtainSession();
        RMNodeDTO rmnodeDTO = session.find(RMNodeDTO.class, hostName);
        if (rmnodeDTO == null) {
            throw new StorageException("Error while retrieving row:" + hostName);
        }
        return createHopRMNode(rmnodeDTO);
    }

  @Override
  public Map<String, HopRMNode> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<RMNodeDTO> dobj = qb.createQueryDefinition(RMNodeDTO.class);
    HopsQuery<RMNodeDTO> query = session.createQuery(dobj);

    List<RMNodeDTO> results = query.getResultList();
    return createMap(results);
  }
    
    
    @Override
    public void deleteAll(int startId, int endId) throws StorageException {
        HopsSession session = connector.obtainSession();
        for (int i = startId; i < endId; i++) {
            RMNodeDTO rmnodeDTO = session.find(RMNodeDTO.class, i);
            session.deletePersistent(rmnodeDTO);
        }
        //session.deletePersistentAll(RMNodeDTO.class);
    }

    @Override
    public void prepare(Collection<HopRMNode> modified, Collection<HopRMNode> removed) throws StorageException {
        HopsSession session = connector.obtainSession();
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
            throw new StorageException("Error while rmnode table:" + e.getMessage());
        }
    }

    @Override
    public void createRMNode(HopRMNode rmNode) throws StorageException {
        HopsSession session = connector.obtainSession();
        session.savePersistent(createPersistable(rmNode, session));
    }

    private RMNodeDTO createPersistable(HopRMNode hopRMNode, HopsSession session) throws StorageException {
        RMNodeDTO rmDTO = session.newInstance(RMNodeDTO.class);
        //Set values to persist new rmnode
        rmDTO.setNodeid(hopRMNode.getNodeId());
        rmDTO.setHostname(hopRMNode.getHostName());
        rmDTO.setCommandport(hopRMNode.getCommandPort());
        rmDTO.setHttpport(hopRMNode.getHttpPort());
        rmDTO.setNodeaddress(hopRMNode.getNodeAddress());
        rmDTO.setHttpaddress(hopRMNode.getHttpAddress());
        rmDTO.setNextheartbeat(NdbBoolean.convert(hopRMNode.isNextHeartbeat()));
        rmDTO.setHealthreport(hopRMNode.getHealthReport());
        rmDTO.setLasthealthreporttime(hopRMNode.getLastHealthReportTime());
        rmDTO.setcurrentstate(hopRMNode.getCurrentState());
        rmDTO.setovercommittimeout(hopRMNode.getOvercommittimeout());
        rmDTO.setnodemanagerversion(hopRMNode.getNodemanagerVersion());
        return rmDTO;
    }

    /**
     * Transforms a DTO to Hop object.
     *
     * @param rmDTO
     * @return HopRMNode
     */
    private HopRMNode createHopRMNode(RMNodeDTO rmDTO) {
        return new HopRMNode(rmDTO.getNodeid(), rmDTO.getHostname(), rmDTO.getCommandport(), 
                rmDTO.getHttpport(), rmDTO.getNodeaddress(), rmDTO.getHttpaddress(),
                NdbBoolean.convert(rmDTO.getNextheartbeat()), rmDTO.getHealthreport(),
                /*rmDTO.getRMContextid(),*/ rmDTO.getLasthealthreporttime(), rmDTO.getcurrentstate(),
                rmDTO.getnodemanagerversion(), rmDTO.getovercommittimeout());
    }
    
  private Map<String, HopRMNode> createMap(List<RMNodeDTO> results) {
    Map<String, HopRMNode> map = new HashMap<String, HopRMNode>();
    for (RMNodeDTO persistable : results) {
      HopRMNode hop = createHopRMNode(persistable);
      map.put(hop.getNodeId(), hop);
    }
    return map;
  }
}
