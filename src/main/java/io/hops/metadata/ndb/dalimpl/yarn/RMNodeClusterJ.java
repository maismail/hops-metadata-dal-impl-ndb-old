package io.hops.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.hops.exception.StorageException;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import io.hops.metadata.hdfs.entity.yarn.HopRMNode;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.yarn.tabledef.RMNodeTableDef;

/**
 * Implements connection of RMNodeImpl to NDB.
 */
public class RMNodeClusterJ implements RMNodeTableDef,
    RMNodeDataAccess<HopRMNode> {

  private static final Log LOG = LogFactory.getLog(RMNodeClusterJ.class);

  @PersistenceCapable(table = TABLE_NAME)
  public interface RMNodeDTO extends RMNodeComponentDTO {

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

    @Column(name = UCI_ID)
    int getuciId();

    void setuciId(int uciId);

  }
  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public HopRMNode findByNodeId(String nodeid) throws StorageException {
    LOG.debug("HOP :: ClusterJ RMNode.findByNodeId - START:" + nodeid);
    HopsSession session = connector.obtainSession();
    RMNodeDTO rmnodeDTO = session.find(RMNodeDTO.class, nodeid);
    if (rmnodeDTO != null) {
      LOG.debug("HOP :: ClusterJ RMNode.findByNodeId - FINISH:" + nodeid);
      return createHopRMNode(rmnodeDTO);
    }
    LOG.debug("HOP :: ClusterJ RMNode.findByNodeId - FINISH:" + nodeid);
    return null;
  }

  @Override
  public Map<String, HopRMNode> getAll() throws StorageException {
    LOG.debug("HOP :: ClusterJ RMNode.getAll - START");
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<RMNodeDTO> dobj = qb.createQueryDefinition(
            RMNodeDTO.class);
    HopsQuery<RMNodeDTO> query = session.createQuery(dobj);
    List<RMNodeDTO> results = query.getResultList();
    LOG.debug("HOP :: ClusterJ RMNode.getAll - FINISH");
    return createMap(results);
  }

  @Override
  public void addAll(Collection<HopRMNode> toAdd) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<RMNodeDTO> toPersist = new ArrayList<RMNodeDTO>();
    for (HopRMNode req : toAdd) {
      toPersist.add(createPersistable(req, session));
    }
    session.savePersistentAll(toPersist);
    session.flush();
  }

  @Override
  public void removeAll(Collection<HopRMNode> toRemove) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<RMNodeDTO> toPersist = new ArrayList<RMNodeDTO>();
    for (HopRMNode entry : toRemove) {
      toPersist.add(session.newInstance(RMNodeDTO.class, entry.
              getNodeId()));
    }
    session.deletePersistentAll(toPersist);
    session.flush();
  }

  @Override
  public void add(HopRMNode rmNode) throws StorageException {
    HopsSession session = connector.obtainSession();
    session.savePersistent(createPersistable(rmNode, session));
  }

  private Map<String, HopRMNode> createMap(List<RMNodeDTO> results) {
    Map<String, HopRMNode> map = new HashMap<String, HopRMNode>();
    for (RMNodeDTO persistable : results) {
      HopRMNode hop = createHopRMNode(persistable);
      map.put(hop.getNodeId(), hop);
    }
    return map;
  }


  private RMNodeDTO createPersistable(HopRMNode hopRMNode, HopsSession session)
          throws StorageException {
    RMNodeDTO rmDTO = session.newInstance(RMNodeDTO.class);
    //Set values to persist new rmnode
    rmDTO.setNodeid(hopRMNode.getNodeId());
    rmDTO.setHostname(hopRMNode.getHostName());
    rmDTO.setCommandport(hopRMNode.getCommandPort());
    rmDTO.setHttpport(hopRMNode.getHttpPort());
    rmDTO.setNodeaddress(hopRMNode.getNodeAddress());
    rmDTO.setHttpaddress(hopRMNode.getHttpAddress());
    rmDTO.setHealthreport(hopRMNode.getHealthReport());
    rmDTO.setLasthealthreporttime(hopRMNode.getLastHealthReportTime());
    rmDTO.setcurrentstate(hopRMNode.getCurrentState());
    rmDTO.setovercommittimeout(hopRMNode.getOvercommittimeout());
    rmDTO.setnodemanagerversion(hopRMNode.getNodemanagerVersion());
    rmDTO.setuciId(hopRMNode.getUciId());
    return rmDTO;
  }

  /**
   * Transforms a DTO to Hop object.
   *
   * @param rmDTO
   * @return HopRMNode
   */
  public static HopRMNode createHopRMNode(RMNodeDTO rmDTO) {
    return new HopRMNode(rmDTO.getNodeid(),
            rmDTO.getHostname(),
            rmDTO.getCommandport(),
            rmDTO.getHttpport(),
            rmDTO.getNodeaddress(),
            rmDTO.getHttpaddress(),
            rmDTO.getHealthreport(),
            rmDTO.getLasthealthreporttime(),
            rmDTO.getcurrentstate(),
            rmDTO.getnodemanagerversion(),
            rmDTO.getovercommittimeout(),
            rmDTO.getuciId());
  }
}
