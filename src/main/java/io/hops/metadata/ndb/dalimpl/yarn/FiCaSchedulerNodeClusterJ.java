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
import io.hops.metadata.yarn.dal.FiCaSchedulerNodeDataAccess;
import io.hops.metadata.yarn.entity.FiCaSchedulerNode;
import io.hops.metadata.yarn.tabledef.FiCaSchedulerNodeTableDef;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class FiCaSchedulerNodeClusterJ implements FiCaSchedulerNodeTableDef,
    FiCaSchedulerNodeDataAccess<FiCaSchedulerNode> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface FiCaSchedulerNodeDTO {

    @PrimaryKey
    @Column(name = RMNODEID)
    String getrmnodeid();

    void setrmnodeid(String rmnodeid);

    @Column(name = NODENAME)
    String getnodename();

    void setnodename(String nodename);

    @Column(name = NUMCONTAINERS)
    int getnumcontainers();

    void setnumcontainers(int numcontainers);

  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override

  public void add(FiCaSchedulerNode toAdd) throws StorageException {
    HopsSession session = connector.obtainSession();
    FiCaSchedulerNodeDTO persistable = createPersistable(toAdd, session);
    session.savePersistent(persistable);
  }

  @Override
  public void addAll(Collection<FiCaSchedulerNode> toAdd)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<FiCaSchedulerNodeDTO> toPersist =
        new ArrayList<FiCaSchedulerNodeDTO>();
    for (FiCaSchedulerNode hop : toAdd) {
      FiCaSchedulerNodeDTO persistable = createPersistable(hop, session);
      toPersist.add(persistable);

      session.savePersistentAll(toPersist);
    }
  }

  @Override
  public void removeAll(Collection<FiCaSchedulerNode> toRemove)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<FiCaSchedulerNodeDTO> toPersist =
        new ArrayList<FiCaSchedulerNodeDTO>();
    for (FiCaSchedulerNode hop : toRemove) {
      FiCaSchedulerNodeDTO persistable =
          session.newInstance(FiCaSchedulerNodeDTO.class, hop.getRmnodeId());
      toPersist.add(persistable);
    }
    session.deletePersistentAll(toPersist);
  }

  @Override
  public List<FiCaSchedulerNode> getAll() throws StorageException {
    try {
      HopsSession session = connector.obtainSession();
      HopsQueryBuilder qb = session.getQueryBuilder();

      HopsQueryDomainType<FiCaSchedulerNodeDTO> dobj = qb.createQueryDefinition(
          FiCaSchedulerNodeClusterJ.FiCaSchedulerNodeDTO.class);
      HopsQuery<FiCaSchedulerNodeClusterJ.FiCaSchedulerNodeDTO> query =
          session.createQuery(dobj);

      List<FiCaSchedulerNodeClusterJ.FiCaSchedulerNodeDTO> results =
          query.getResultList();
      return createFiCaSchedulerNodeList(results);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }


  private FiCaSchedulerNodeDTO createPersistable(FiCaSchedulerNode hop,
      HopsSession session) throws StorageException {
    FiCaSchedulerNodeDTO ficaDTO =
        session.newInstance(FiCaSchedulerNodeDTO.class);
    ficaDTO.setrmnodeid(hop.getRmnodeId());
    ficaDTO.setnodename(hop.getNodeName());
    ficaDTO.setnumcontainers(hop.getNumOfContainers());
    return ficaDTO;
  }

  private List<FiCaSchedulerNode> createFiCaSchedulerNodeList(
      List<FiCaSchedulerNodeClusterJ.FiCaSchedulerNodeDTO> results) {
    List<FiCaSchedulerNode> fifoSchedulerNodes =
        new ArrayList<FiCaSchedulerNode>();
    for (FiCaSchedulerNodeClusterJ.FiCaSchedulerNodeDTO persistable : results) {
      fifoSchedulerNodes.add(createHopFiCaSchedulerNode(persistable));
    }
    return fifoSchedulerNodes;
  }

  private FiCaSchedulerNode createHopFiCaSchedulerNode(
      FiCaSchedulerNodeDTO entry) {
    FiCaSchedulerNode hop =
        new FiCaSchedulerNode(entry.getrmnodeid(), entry.getnodename(),
            entry.getnumcontainers());
    return hop;
  }
}
