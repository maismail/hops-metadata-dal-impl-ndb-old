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
import io.hops.metadata.yarn.dal.RMContextInactiveNodesDataAccess;
import io.hops.metadata.yarn.entity.RMContextInactiveNodes;
import io.hops.metadata.yarn.tabledef.RMContextInactiveNodesTableDef;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class RMContextInactiveNodesClusterJ
    implements RMContextInactiveNodesTableDef,
    RMContextInactiveNodesDataAccess<RMContextInactiveNodes> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface RMContextInactiveNodesDTO {

    @PrimaryKey
    @Column(name = RMNODEID)
    String getrmnodeid();

    void setrmnodeid(String rmnodeid);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public List<RMContextInactiveNodes> findAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<RMContextInactiveNodesDTO> dobj = qb.
        createQueryDefinition(RMContextInactiveNodesDTO.class);
    HopsQuery<RMContextInactiveNodesDTO> query = session.createQuery(dobj);

    List<RMContextInactiveNodesDTO> results = query.getResultList();
    return createRMContextInactiveNodesList(results);
  }

  @Override
  public void addAll(Collection<RMContextInactiveNodes> toAdd)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<RMContextInactiveNodesDTO> toPersist =
        new ArrayList<RMContextInactiveNodesDTO>();
    for (RMContextInactiveNodes req : toAdd) {
      toPersist.add(createPersistable(req, session));
    }
    session.savePersistentAll(toPersist);
    session.flush();
  }

  @Override
  public void removeAll(Collection<RMContextInactiveNodes> toRemove)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<RMContextInactiveNodesDTO> toPersist =
        new ArrayList<RMContextInactiveNodesDTO>();
    for (RMContextInactiveNodes entry : toRemove) {
      toPersist.add(session.newInstance(RMContextInactiveNodesDTO.class, entry.
          getRmnodeid()));
    }
    session.deletePersistentAll(toPersist);
    session.flush();
  }

  private RMContextInactiveNodes createRMContextInactiveNodesEntry(
      RMContextInactiveNodesDTO entry) {
    return new RMContextInactiveNodes(entry.getrmnodeid());
  }

  private RMContextInactiveNodesDTO createPersistable(
      RMContextInactiveNodes entry, HopsSession session)
      throws StorageException {
    RMContextInactiveNodesDTO persistable =
        session.newInstance(RMContextInactiveNodesDTO.class);
    persistable.setrmnodeid(entry.getRmnodeid());
    return persistable;
  }

  private List<RMContextInactiveNodes> createRMContextInactiveNodesList(
      List<RMContextInactiveNodesDTO> results) {
    List<RMContextInactiveNodes> rmcontextInactiveNodes =
        new ArrayList<RMContextInactiveNodes>();
    for (RMContextInactiveNodesDTO persistable : results) {
      rmcontextInactiveNodes
          .add(createRMContextInactiveNodesEntry(persistable));
    }
    return rmcontextInactiveNodes;
  }
}
