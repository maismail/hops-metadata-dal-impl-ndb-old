package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.google.common.primitives.Ints;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.dal.PendingBlockDataAccess;
import se.sics.hop.metadata.hdfs.entity.hdfs.HopPendingBlockInfo;
import se.sics.hop.metadata.hdfs.tabledef.PendingBlockTableDef;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.mysqlserver.MySQLQueryHelper;
import se.sics.hop.metadata.ndb.wrapper.HopsPredicate;
import se.sics.hop.metadata.ndb.wrapper.HopsPredicateOperand;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class PendingBlockClusterj implements PendingBlockTableDef, PendingBlockDataAccess<HopPendingBlockInfo> {

  @Override
  public int countValidPendingBlocks(long timeLimit) throws StorageException {
    return MySQLQueryHelper.countWithCriterion(TABLE_NAME, String.format("%s>%d", TIME_STAMP, timeLimit));
  }

  @Override
  public List<HopPendingBlockInfo> findByINodeId(int inodeId) throws StorageException {
    HopsSession session = connector.obtainSession();

    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<PendingBlockDTO> qdt = qb.createQueryDefinition(PendingBlockDTO.class);

    HopsPredicate pred1 = qdt.get("iNodeId").equal(qdt.param("idParam"));
    qdt.where(pred1);

    HopsQuery<PendingBlockDTO> query = session.createQuery(qdt);
    query.setParameter("idParam", inodeId);

    return createList(query.getResultList());
  }

  @Override
  public List<HopPendingBlockInfo> findByINodeIds(int[] inodeIds) throws StorageException {
    HopsSession session = connector.obtainSession();

    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<PendingBlockDTO> qdt = qb.createQueryDefinition(PendingBlockDTO.class);

    HopsPredicate pred1 = qdt.get("iNodeId").in(qdt.param("idParam"));
    qdt.where(pred1);

    HopsQuery<PendingBlockDTO> query = session.createQuery(qdt);
    query.setParameter("idParam", Ints.asList(inodeIds));

    return createList(query.getResultList());
  }

  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column=INODE_ID)
  public interface PendingBlockDTO {

    @PrimaryKey
    @Column(name = INODE_ID)
    int getINodeId();
    void setINodeId(int inodeId);
    
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
    HopsSession session = connector.obtainSession();
    List<PendingBlockDTO> changes = new ArrayList<PendingBlockDTO>();
    List<PendingBlockDTO> deletions = new ArrayList<PendingBlockDTO>();
    for (HopPendingBlockInfo p : newed) {
      PendingBlockDTO pTable = session.newInstance(PendingBlockDTO.class);
      createPersistableHopPendingBlockInfo(p, pTable);
      changes.add(pTable);
    }
    for (HopPendingBlockInfo p : modified) {
      PendingBlockDTO pTable = session.newInstance(PendingBlockDTO.class);
      createPersistableHopPendingBlockInfo(p, pTable);
      changes.add(pTable);
    }

    for (HopPendingBlockInfo p : removed) {
      PendingBlockDTO pTable = session.newInstance(PendingBlockDTO.class);
      createPersistableHopPendingBlockInfo(p, pTable);
      deletions.add(pTable);
    }
    session.deletePersistentAll(deletions);
    session.savePersistentAll(changes);
  }

  @Override
  public HopPendingBlockInfo findByPKey(long blockId, int inodeId) throws StorageException {
    HopsSession session = connector.obtainSession();
    Object[] pk = new Object[2];
    pk[0] = inodeId;
    pk[1] = blockId;

    PendingBlockDTO pendingTable = session.find(PendingBlockDTO.class, pk);
    HopPendingBlockInfo pendingBlock = null;
    if (pendingTable != null) {
      pendingBlock = createHopPendingBlockInfo(pendingTable);
    }

    return pendingBlock;
  }

  @Override
  public List<HopPendingBlockInfo> findAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQuery<PendingBlockDTO> query =
        session.createQuery(
            qb.createQueryDefinition(PendingBlockDTO.class));
    return createList(query.getResultList());
  }

  @Override
  public List<HopPendingBlockInfo> findByTimeLimitLessThan(long timeLimit) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<PendingBlockDTO> qdt = qb.createQueryDefinition(PendingBlockDTO.class);
    HopsPredicateOperand predicateOp = qdt.get("timestamp");
    String paramName = "timelimit";
    HopsPredicateOperand param = qdt.param(paramName);
    HopsPredicate lessThan = predicateOp.lessThan(param);
    qdt.where(lessThan);
    HopsQuery query = session.createQuery(qdt);
    query.setParameter(paramName, timeLimit);
    return createList(query.getResultList());
  }

  @Override
  public void removeAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    session.deletePersistentAll(PendingBlockDTO.class);
  }

  private List<HopPendingBlockInfo> createList(Collection<PendingBlockDTO> dtos) {
    List<HopPendingBlockInfo> list = new ArrayList<HopPendingBlockInfo>();
    for (PendingBlockDTO dto : dtos) {
      list.add(createHopPendingBlockInfo(dto));
    }
    return list;
  }

  private HopPendingBlockInfo createHopPendingBlockInfo(PendingBlockDTO pendingTable) {
    return new HopPendingBlockInfo(pendingTable.getBlockId(),pendingTable.getINodeId(),
        pendingTable.getTimestamp(), pendingTable.getNumReplicasInProgress());
  }

  private void createPersistableHopPendingBlockInfo(HopPendingBlockInfo pendingBlock, PendingBlockDTO pendingTable) {
    pendingTable.setBlockId(pendingBlock.getBlockId());
    pendingTable.setNumReplicasInProgress(pendingBlock.getNumReplicas());
    pendingTable.setTimestamp(pendingBlock.getTimeStamp());
    pendingTable.setINodeId(pendingBlock.getInodeId());
  }
}
