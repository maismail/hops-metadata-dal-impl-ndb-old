package io.hops.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.RMContextActiveNodesDataAccess;
import io.hops.metadata.yarn.entity.RMContextActiveNodes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class RMContextActiveNodesClusterJ
    implements TablesDef.RMContextActiveNodesTableDef,
    RMContextActiveNodesDataAccess<RMContextActiveNodes> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface RMContextNodesDTO {

    @PrimaryKey
    @Column(name = RMNODEID)
    String getnodeidid();

    void setnodeidid(String nodeidid);

  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public RMContextActiveNodes findEntry(String nodeidId)
      throws StorageException {
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
  public List<RMContextActiveNodes> findAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<RMContextNodesDTO> dobj =
        qb.createQueryDefinition(RMContextNodesDTO.class);
    HopsQuery<RMContextNodesDTO> query = session.createQuery(dobj);

    List<RMContextNodesDTO> results = query.getResultList();
    if (results != null && !results.isEmpty()) {
      return createRMContextNodesList(results);
    }
    return null;
  }

  @Override
  public void addAll(Collection<RMContextActiveNodes> toAdd)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<RMContextNodesDTO> toPersist = new ArrayList<RMContextNodesDTO>();
    for (RMContextActiveNodes req : toAdd) {
      toPersist.add(createPersistable(req, session));
    }
    session.savePersistentAll(toPersist);
    session.flush();
  }

  @Override
  public void removeAll(Collection<RMContextActiveNodes> toRemove)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<RMContextNodesDTO> toPersist = new ArrayList<RMContextNodesDTO>();
    for (RMContextActiveNodes entry : toRemove) {
      toPersist.add(session.newInstance(RMContextNodesDTO.class, entry.
          getNodeId()));
    }
    session.deletePersistentAll(toPersist);
    session.flush();
  }

  private RMContextNodesDTO createPersistable(RMContextActiveNodes entry,
      HopsSession session) throws StorageException {
    RMContextNodesDTO persistable =
        session.newInstance(RMContextNodesDTO.class, entry.getNodeId());
    persistable.setnodeidid(entry.getNodeId());
    //session.savePersistent(persistable);
    return persistable;
  }

  private RMContextActiveNodes createRMContextNodesEntry(
      RMContextNodesDTO entry) {
    return new RMContextActiveNodes(entry.getnodeidid());
  }

  private List<RMContextActiveNodes> createRMContextNodesList(
      List<RMContextNodesDTO> results) {
    List<RMContextActiveNodes> rmcontextNodes =
        new ArrayList<RMContextActiveNodes>();
    for (RMContextNodesDTO persistable : results) {
      rmcontextNodes.add(createRMContextNodesEntry(persistable));
    }
    return rmcontextNodes;
  }
}
