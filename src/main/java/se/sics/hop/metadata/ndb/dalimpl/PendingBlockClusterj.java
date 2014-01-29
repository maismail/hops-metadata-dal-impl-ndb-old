package se.sics.hop.metadata.ndb.dalimpl;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.PredicateOperand;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.metadata.hdfs.dal.PendingBlockDataAccess;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.hdfs.HopPendingBlockInfo;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.mysqlserver.CountHelper;
import se.sics.hop.metadata.hdfs.tabledef.PendingBlockTableDef;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class PendingBlockClusterj implements PendingBlockTableDef, PendingBlockDataAccess<HopPendingBlockInfo> {

  @Override
  public int countValidPendingBlocks(long timeLimit) throws StorageException {
    return CountHelper.countWithCriterion(TABLE_NAME, String.format("%s>%d", TIME_STAMP, timeLimit));
  }

  @PersistenceCapable(table = TABLE_NAME)
  public interface PendingBlockDTO {

    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();

    void setBlockId(long blockId);

    @Column(name = TIME_STAMP)
    long getTimestamp();

    void setTimestamp(long timestamp);

    @Column(name = NUM_REPLICAS_IN_PROGRESS)
    int getNumReplicasInProgress();

    void setNumReplicasInProgress(int numReplicasInProgress);
  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public void prepare(Collection<HopPendingBlockInfo> removed, Collection<HopPendingBlockInfo> newed, Collection<HopPendingBlockInfo> modified) throws StorageException {
    Session session = connector.obtainSession();
    for (HopPendingBlockInfo p : newed) {
      PendingBlockDTO pTable = session.newInstance(PendingBlockDTO.class);
      createPersistableHopPendingBlockInfo(p, pTable);
      session.savePersistent(pTable);
    }
    for (HopPendingBlockInfo p : modified) {
      PendingBlockDTO pTable = session.newInstance(PendingBlockDTO.class);
      createPersistableHopPendingBlockInfo(p, pTable);
      session.savePersistent(pTable);
    }

    for (HopPendingBlockInfo p : removed) {
      PendingBlockDTO pTable = session.newInstance(PendingBlockDTO.class, p.getBlockId());
      session.deletePersistent(pTable);
    }
  }

  @Override
  public HopPendingBlockInfo findByPKey(long blockId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      PendingBlockDTO pendingTable = session.find(PendingBlockDTO.class, blockId);
      HopPendingBlockInfo pendingBlock = null;
      if (pendingTable != null) {
        pendingBlock = createHopPendingBlockInfo(pendingTable);
      }

      return pendingBlock;
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopPendingBlockInfo> findAll() throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      Query<PendingBlockDTO> query =
              session.createQuery(qb.createQueryDefinition(PendingBlockDTO.class));
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopPendingBlockInfo> findByTimeLimitLessThan(long timeLimit) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<PendingBlockDTO> qdt = qb.createQueryDefinition(PendingBlockDTO.class);
      PredicateOperand predicateOp = qdt.get("timestamp");
      String paramName = "timelimit";
      PredicateOperand param = qdt.param(paramName);
      Predicate lessThan = predicateOp.lessThan(param);
      qdt.where(lessThan);
      Query query = session.createQuery(qdt);
      query.setParameter(paramName, timeLimit);
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void removeAll() throws StorageException {
    try {
      Session session = connector.obtainSession();
      session.deletePersistentAll(PendingBlockDTO.class);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private List<HopPendingBlockInfo> createList(Collection<PendingBlockDTO> dtos) {
    List<HopPendingBlockInfo> list = new ArrayList<HopPendingBlockInfo>();
    for (PendingBlockDTO dto : dtos) {
      list.add(createHopPendingBlockInfo(dto));
    }
    return list;
  }

  private HopPendingBlockInfo createHopPendingBlockInfo(PendingBlockDTO pendingTable) {
    return new HopPendingBlockInfo(pendingTable.getBlockId(),
            pendingTable.getTimestamp(), pendingTable.getNumReplicasInProgress());
  }

  private void createPersistableHopPendingBlockInfo(HopPendingBlockInfo pendingBlock, PendingBlockDTO pendingTable) {
    pendingTable.setBlockId(pendingBlock.getBlockId());
    pendingTable.setNumReplicasInProgress(pendingBlock.getNumReplicas());
    pendingTable.setTimestamp(pendingBlock.getTimeStamp());
  }
}
