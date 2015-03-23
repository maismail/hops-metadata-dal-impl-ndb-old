package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopFiCaSchedulerNode;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.FiCaSchedulerNodeDataAccess;
import se.sics.hop.metadata.yarn.tabledef.FiCaSchedulerNodeTableDef;

public class FiCaSchedulerNodeClusterJ implements FiCaSchedulerNodeTableDef, FiCaSchedulerNodeDataAccess<HopFiCaSchedulerNode> {

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

  public void add(HopFiCaSchedulerNode toAdd) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    FiCaSchedulerNodeDTO persistable = createPersistable(toAdd, session);
    session.savePersistent(persistable);
  }

  @Override
  public void addAll(Collection<HopFiCaSchedulerNode> toAdd) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<FiCaSchedulerNodeDTO> toPersist = new ArrayList<FiCaSchedulerNodeDTO>();
    for (HopFiCaSchedulerNode hop : toAdd) {
      FiCaSchedulerNodeDTO persistable = createPersistable(hop, session);
      toPersist.add(persistable);

      session.savePersistentAll(toPersist);
    }
  }

  @Override
  public void removeAll(Collection<HopFiCaSchedulerNode> toRemove) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<FiCaSchedulerNodeDTO> toPersist = new ArrayList<FiCaSchedulerNodeDTO>();
    for (HopFiCaSchedulerNode hop : toRemove) {
      FiCaSchedulerNodeDTO persistable = session.newInstance(
              FiCaSchedulerNodeDTO.class, hop.getRmnodeId());
      toPersist.add(persistable);
    }
    session.deletePersistentAll(toPersist);
  }

      @Override
    public List<HopFiCaSchedulerNode> getAll() throws StorageException {
        try {
            HopsSession session = connector.obtainSession();
            HopsQueryBuilder qb = session.getQueryBuilder();

            HopsQueryDomainType<FiCaSchedulerNodeClusterJ.FiCaSchedulerNodeDTO> dobj = qb.createQueryDefinition(FiCaSchedulerNodeClusterJ.FiCaSchedulerNodeDTO.class);
            HopsQuery<FiCaSchedulerNodeClusterJ.FiCaSchedulerNodeDTO> query = session.createQuery(dobj);

            List<FiCaSchedulerNodeClusterJ.FiCaSchedulerNodeDTO> results = query.getResultList();
            return createFiCaSchedulerNodeList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    
    private FiCaSchedulerNodeDTO createPersistable(HopFiCaSchedulerNode hop, HopsSession session) throws StorageException {
        FiCaSchedulerNodeDTO ficaDTO = session.newInstance(FiCaSchedulerNodeDTO.class);
        ficaDTO.setrmnodeid(hop.getRmnodeId());
        ficaDTO.setnodename(hop.getNodeName());
        ficaDTO.setnumcontainers(hop.getNumOfContainers());
        return ficaDTO;
    }

  private List<HopFiCaSchedulerNode> createFiCaSchedulerNodeList(
          List<FiCaSchedulerNodeClusterJ.FiCaSchedulerNodeDTO> results) {
    List<HopFiCaSchedulerNode> fifoSchedulerNodes
            = new ArrayList<HopFiCaSchedulerNode>();
    for (FiCaSchedulerNodeClusterJ.FiCaSchedulerNodeDTO persistable : results) {
      fifoSchedulerNodes.add(createHopFiCaSchedulerNode(persistable));
    }
    return fifoSchedulerNodes;
  }

  private HopFiCaSchedulerNode createHopFiCaSchedulerNode(
          FiCaSchedulerNodeDTO entry) {
    HopFiCaSchedulerNode hop = new HopFiCaSchedulerNode(entry.getrmnodeid(),
            entry.getnodename(), entry.getnumcontainers());
    return hop;
  }
}
