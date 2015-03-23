package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopRMContextInactiveNodes;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.RMContextInactiveNodesDataAccess;
import se.sics.hop.metadata.yarn.tabledef.RMContextInactiveNodesTableDef;

public class RMContextInactiveNodesClusterJ implements
        RMContextInactiveNodesTableDef,
        RMContextInactiveNodesDataAccess<HopRMContextInactiveNodes> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface RMContextInactiveNodesDTO {

    @PrimaryKey
    @Column(name = RMNODEID)
    String getrmnodeid();

    void setrmnodeid(String rmnodeid);
  }
  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public List<HopRMContextInactiveNodes> findAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<RMContextInactiveNodesDTO> dobj = qb.
            createQueryDefinition(RMContextInactiveNodesDTO.class);
    HopsQuery<RMContextInactiveNodesDTO> query = session.createQuery(dobj);

    List<RMContextInactiveNodesDTO> results = query.getResultList();
    return createRMContextInactiveNodesList(results);
  }

  @Override
  public void addAll(Collection<HopRMContextInactiveNodes> toAdd) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<RMContextInactiveNodesDTO> toPersist
            = new ArrayList<RMContextInactiveNodesDTO>();
    for (HopRMContextInactiveNodes req : toAdd) {
      toPersist.add(createPersistable(req, session));
    }
    session.savePersistentAll(toPersist);
    session.flush();
  }

  @Override
  public void removeAll(Collection<HopRMContextInactiveNodes> toRemove) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<RMContextInactiveNodesDTO> toPersist
            = new ArrayList<RMContextInactiveNodesDTO>();
    for (HopRMContextInactiveNodes entry : toRemove) {
      toPersist.add(session.newInstance(RMContextInactiveNodesDTO.class, entry.
              getRmnodeid()));
    }
    session.deletePersistentAll(toPersist);
    session.flush();
  }

  private HopRMContextInactiveNodes createRMContextInactiveNodesEntry(
          RMContextInactiveNodesDTO entry) {
    return new HopRMContextInactiveNodes(entry.getrmnodeid());
  }

  private RMContextInactiveNodesDTO createPersistable(
          HopRMContextInactiveNodes entry, HopsSession session) throws
          StorageException {
    RMContextInactiveNodesDTO persistable = session.newInstance(
            RMContextInactiveNodesDTO.class);
    persistable.setrmnodeid(entry.getRmnodeid());
    return persistable;
  }

  private List<HopRMContextInactiveNodes> createRMContextInactiveNodesList(
          List<RMContextInactiveNodesDTO> results) {
    List<HopRMContextInactiveNodes> rmcontextInactiveNodes
            = new ArrayList<HopRMContextInactiveNodes>();
    for (RMContextInactiveNodesDTO persistable : results) {
      rmcontextInactiveNodes.add(createRMContextInactiveNodesEntry(persistable));
    }
    return rmcontextInactiveNodes;
  }
}
