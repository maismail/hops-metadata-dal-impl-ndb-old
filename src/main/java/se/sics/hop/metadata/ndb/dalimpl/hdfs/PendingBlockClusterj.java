package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.PredicateOperand;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDefinition;
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
import static se.sics.hop.metadata.hdfs.tabledef.PendingBlockTableDef.INODE_ID;
import se.sics.hop.metadata.ndb.DBSession;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class PendingBlockClusterj implements PendingBlockTableDef, PendingBlockDataAccess<HopPendingBlockInfo> {

  @Override
  public int countValidPendingBlocks(long timeLimit) throws StorageException {
    return CountHelper.countWithCriterion(TABLE_NAME, String.format("%s>%d", TIME_STAMP, timeLimit));
  }

  @Override
  public List<HopPendingBlockInfo> findByINodeId(int inodeId) throws StorageException {
    try {
      DBSession session = connector.obtainSession();
      
      QueryBuilder qb = session.getSession().getQueryBuilder();
      QueryDomainType<PendingBlockDTO> qdt = qb.createQueryDefinition(PendingBlockDTO.class);
      
      Predicate pred1 = qdt.get("iNodeId").equal(qdt.param("idParam"));
      qdt.where(pred1);

      Query<PendingBlockDTO> query = session.getSession().createQuery(qdt);
      query.setParameter("idParam", inodeId);
     
      List<PendingBlockDTO> results = query.getResultList();
 
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
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
    DBSession session = connector.obtainSession();
    List<PendingBlockDTO> changes = new ArrayList<PendingBlockDTO>();
    List<PendingBlockDTO> deletions = new ArrayList<PendingBlockDTO>();
    for (HopPendingBlockInfo p : newed) {
      PendingBlockDTO pTable = session.getSession().newInstance(PendingBlockDTO.class);
      createPersistableHopPendingBlockInfo(p, pTable);
      changes.add(pTable);
    }
    for (HopPendingBlockInfo p : modified) {
      PendingBlockDTO pTable = session.getSession().newInstance(PendingBlockDTO.class);
      createPersistableHopPendingBlockInfo(p, pTable);
      changes.add(pTable);
    }

    for (HopPendingBlockInfo p : removed) {
      PendingBlockDTO pTable = session.getSession().newInstance(PendingBlockDTO.class);
      createPersistableHopPendingBlockInfo(p, pTable);
      deletions.add(pTable);
    }
    session.getSession().deletePersistentAll(deletions);
    session.getSession().savePersistentAll(changes);
  }

  @Override
  public HopPendingBlockInfo findByPKey(long blockId, int inodeId) throws StorageException {
    try {
      DBSession session = connector.obtainSession();
      Object[] pk = new Object[2];
      pk[0] = inodeId;
      pk[1] = blockId;

      PendingBlockDTO pendingTable = session.getSession().find(PendingBlockDTO.class, pk);
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
      DBSession session = connector.obtainSession();
      QueryBuilder qb = session.getSession().getQueryBuilder();
      Query<PendingBlockDTO> query =
              session.getSession().createQuery(qb.createQueryDefinition(PendingBlockDTO.class));
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopPendingBlockInfo> findByTimeLimitLessThan(long timeLimit) throws StorageException {
    try {
      DBSession session = connector.obtainSession();
      QueryBuilder qb = session.getSession().getQueryBuilder();
      QueryDomainType<PendingBlockDTO> qdt = qb.createQueryDefinition(PendingBlockDTO.class);
      PredicateOperand predicateOp = qdt.get("timestamp");
      String paramName = "timelimit";
      PredicateOperand param = qdt.param(paramName);
      Predicate lessThan = predicateOp.lessThan(param);
      qdt.where(lessThan);
      Query query = session.getSession().createQuery(qdt);
      query.setParameter(paramName, timeLimit);
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void removeAll() throws StorageException {
    try {
      DBSession session = connector.obtainSession();
      session.getSession().deletePersistentAll(PendingBlockDTO.class);
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
