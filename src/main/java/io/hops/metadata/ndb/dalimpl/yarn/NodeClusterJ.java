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
import io.hops.metadata.yarn.entity.HopNode;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.dal.NodeDataAccess;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.yarn.tabledef.NodeTableDef;

public class NodeClusterJ implements NodeTableDef, NodeDataAccess<HopNode> {

  private static final Log LOG = LogFactory.getLog(NodeClusterJ.class);

  @PersistenceCapable(table = TABLE_NAME)
  public interface NodeDTO extends RMNodeComponentDTO {

    @PrimaryKey
    @Column(name = NODEID)
    String getnodeid();

    void setnodeid(String id);

    @Column(name = NAME)
    String getName();

    void setName(String host);

    @Column(name = LOCATION)
    String getLocation();

    void setLocation(String location);

    @Column(name = LEVEL)
    int getLevel();

    void setLevel(int level);

    @Column(name = PARENT)
    String getParent();

    void setParent(String parent);
  }
  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public HopNode findById(String id) throws StorageException {
    LOG.debug("HOP :: ClusterJ Node.findById - START:" + id);
    HopsSession session = connector.obtainSession();
    NodeDTO nodeDTO;
    if (session != null) {
      nodeDTO = session.find(NodeDTO.class, id);
      LOG.debug("HOP :: ClusterJ Node.findById - FINISH:" + id);
      if (nodeDTO != null) {
        return createHopNode(nodeDTO);
      }
    }
    return null;
  }

  @Override
  public Map<String, HopNode> getAll() throws StorageException {
    LOG.debug("HOP :: ClusterJ Node.getAll - START");
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<NodeDTO> dobj = qb.createQueryDefinition(NodeDTO.class);
    HopsQuery<NodeDTO> query = session.createQuery(dobj);

    List<NodeDTO> results = query.getResultList();
    LOG.debug("HOP :: ClusterJ Node.getAll - FINISH");
    return createMap(results);
  }

  @Override
  public void addAll(Collection<HopNode> toAdd) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<NodeDTO> toPersist = new ArrayList<NodeDTO>();
    for (HopNode node : toAdd) {
      toPersist.add(createPersistable(node, session));
    }
    session.savePersistentAll(toPersist);
    session.flush();
  }

  @Override
  public void createNode(HopNode node) throws StorageException {
    HopsSession session = connector.obtainSession();
    session.savePersistent(createPersistable(node, session));
  }

  private NodeDTO createPersistable(HopNode hopNode, HopsSession session) throws
          StorageException {
    NodeDTO nodeDTO = session.newInstance(NodeDTO.class);
    //Set values to persist new rmnode
    nodeDTO.setnodeid(hopNode.getId());
    nodeDTO.setName(hopNode.getName());
    nodeDTO.setLocation(hopNode.getLocation());
    nodeDTO.setLevel(hopNode.getLevel());
    nodeDTO.setParent(hopNode.getParent());
    return nodeDTO;
  }

  /**
   * Transforms a DTO to Hop object.
   *
   * <p>
   * @param nodeDTO
   * @return HopRMNode
   */
  public static HopNode createHopNode(NodeDTO nodeDTO) {
    return new HopNode(nodeDTO.getnodeid(), nodeDTO.getName(), nodeDTO.
            getLocation(), nodeDTO.getLevel(), nodeDTO.getParent());
  }

  private Map<String, HopNode> createMap(List<NodeDTO> results) {
    Map<String, HopNode> map = new HashMap<String, HopNode>();
    for (NodeDTO persistable : results) {
      HopNode hop = createHopNode(persistable);
      map.put(hop.getId(), hop);
    }
    return map;
  }
}
