package io.hops.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import io.hops.exception.StorageException;
import io.hops.metadata.yarn.entity.HopRMContextActiveNodes;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.dal.RMContextActiveNodesDataAccess;
import io.hops.metadata.yarn.tabledef.RMContextActiveNodesTableDef;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;

public class RMContextActiveNodesClusterJ implements
    RMContextActiveNodesTableDef,
    RMContextActiveNodesDataAccess<HopRMContextActiveNodes> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface RMContextNodesDTO {

    @PrimaryKey
    @Column(name = RMNODEID)
    String getnodeidid();

    void setnodeidid(String nodeidid);

  }
  private final ClusterjConnector connector = ClusterjConnector.getInstance();
 @Override
  public HopRMContextActiveNodes findEntry(String nodeidId) throws
     StorageException {
    HopsSession session = connector.obtainSession();
    if (session != null) {
      RMContextNodesDTO entry = session.find(RMContextNodesDTO.class, nodeidId);
      if (entry != null) {
        return createRMContextNodesEntry(entry);
      }
    }
    return null;
  }
  @Override
  public List<HopRMContextActiveNodes> findAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<RMContextNodesDTO> dobj = qb.createQueryDefinition(
            RMContextNodesDTO.class);
    HopsQuery<RMContextNodesDTO> query = session.createQuery(dobj);

    List<RMContextNodesDTO> results = query.getResultList();
    if (results != null && !results.isEmpty()) {
      return createRMContextNodesList(results);
    }
    return null;
  }

  @Override
  public void addAll(Collection<HopRMContextActiveNodes> toAdd) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<RMContextNodesDTO> toPersist = new ArrayList<RMContextNodesDTO>();
    for (HopRMContextActiveNodes req : toAdd) {
      toPersist.add(createPersistable(req, session));
    }
    session.savePersistentAll(toPersist);
    session.flush();
  }

  @Override
  public void removeAll(Collection<HopRMContextActiveNodes> toRemove) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<RMContextNodesDTO> toPersist = new ArrayList<RMContextNodesDTO>();
    for (HopRMContextActiveNodes entry : toRemove) {
      toPersist.add(session.newInstance(RMContextNodesDTO.class, entry.
              getNodeId()));
    }
    session.deletePersistentAll(toPersist);
    session.flush();
  }

  private RMContextNodesDTO createPersistable(HopRMContextActiveNodes entry,
          HopsSession session) throws StorageException {
    RMContextNodesDTO persistable = session.newInstance(RMContextNodesDTO.class,
            entry.getNodeId());
    persistable.setnodeidid(entry.getNodeId());
    //session.savePersistent(persistable);
    return persistable;
  }

  private HopRMContextActiveNodes createRMContextNodesEntry(
          RMContextNodesDTO entry) {
    return new HopRMContextActiveNodes(entry.getnodeidid());
  }

  private List<HopRMContextActiveNodes> createRMContextNodesList(
          List<RMContextNodesDTO> results) {
    List<HopRMContextActiveNodes> rmcontextNodes
            = new ArrayList<HopRMContextActiveNodes>();
    for (RMContextNodesDTO persistable : results) {
      rmcontextNodes.add(createRMContextNodesEntry(persistable));
    }
    return rmcontextNodes;
  }
}
