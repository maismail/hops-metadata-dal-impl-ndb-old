package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.google.common.primitives.Ints;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.metadata.hdfs.entity.hop.HopUnderReplicatedBlock;
import se.sics.hop.metadata.hdfs.dal.UnderReplicatedBlockDataAccess;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.mysqlserver.MySQLQueryHelper;
import se.sics.hop.metadata.hdfs.tabledef.UnderReplicatedBlockTableDef;
import se.sics.hop.metadata.ndb.DBSession;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public class UnderReplicatedBlockClusterj implements UnderReplicatedBlockTableDef, UnderReplicatedBlockDataAccess<HopUnderReplicatedBlock> {

  @Override
  public int countByLevel(int level) throws StorageException {
    return MySQLQueryHelper.countWithCriterion(
            TABLE_NAME,
            String.format("%s=%d", LEVEL, level));
  }

  @Override
  public int countLessThanALevel(int level) throws StorageException {
    return MySQLQueryHelper.countWithCriterion(
            TABLE_NAME,
            String.format("%s<%d", LEVEL, level));
  }

  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column=INODE_ID)
  @Index(name="level")
  public interface UnderReplicatedBlocksDTO {

    @PrimaryKey
    @Column(name = INODE_ID)
    int getINodeId();
    void setINodeId(int inodeId);
    
    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();
    void setBlockId(long bid);

    @Column(name = LEVEL)
    int getLevel();

    void setLevel(int level);
    
    @Column(name = TIMESTAMP)
    long getTimestamp();
    void setTimestamp(long timestamp);
  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public HopUnderReplicatedBlock findByPk(long blockId, int inodeId) throws StorageException {
    try {
      DBSession dbSession = connector.obtainSession();
      Object[] pk = new Object[2];
        pk[0] = inodeId;
        pk[1] = blockId;
        
      UnderReplicatedBlocksDTO urbt = dbSession.getSession().find(UnderReplicatedBlocksDTO.class, pk);
      if (urbt == null) {
        return null;
      }
      return createUrBlock(urbt);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void prepare(Collection<HopUnderReplicatedBlock> removed, Collection<HopUnderReplicatedBlock> newed, Collection<HopUnderReplicatedBlock> modified) throws StorageException {
    DBSession dbSession = connector.obtainSession();
    List<UnderReplicatedBlocksDTO> changes = new ArrayList<UnderReplicatedBlocksDTO>();
    List<UnderReplicatedBlocksDTO> deletions = new ArrayList<UnderReplicatedBlocksDTO>();
    for (HopUnderReplicatedBlock urBlock : removed) {
      UnderReplicatedBlocksDTO newInstance = dbSession.getSession().newInstance(UnderReplicatedBlocksDTO.class);
      createPersistable(urBlock, newInstance);
      deletions.add(newInstance);
    }

    for (HopUnderReplicatedBlock urBlock : newed) {
      UnderReplicatedBlocksDTO newInstance = dbSession.getSession().newInstance(UnderReplicatedBlocksDTO.class);
      createPersistable(urBlock, newInstance);
      changes.add(newInstance);
    }

    for (HopUnderReplicatedBlock urBlock : modified) {
      UnderReplicatedBlocksDTO newInstance = dbSession.getSession().newInstance(UnderReplicatedBlocksDTO.class);
      createPersistable(urBlock, newInstance);
      changes.add(newInstance);
    }
    dbSession.getSession().deletePersistentAll(deletions);
    dbSession.getSession().savePersistentAll(changes);
  }

  private void createPersistable(HopUnderReplicatedBlock block, UnderReplicatedBlocksDTO persistable) {
    persistable.setBlockId(block.getBlockId());
    persistable.setLevel(block.getLevel());
    persistable.setINodeId(block.getInodeId());
    persistable.setTimestamp(System.currentTimeMillis());
  }

  private HopUnderReplicatedBlock createUrBlock(UnderReplicatedBlocksDTO bit) {
    HopUnderReplicatedBlock block = new HopUnderReplicatedBlock(bit.getLevel(), bit.getBlockId(), bit.getINodeId());
    return block;
  }

  private List<HopUnderReplicatedBlock> createUrBlockList(List<UnderReplicatedBlocksDTO> bitList) {
    List<HopUnderReplicatedBlock> blocks = new ArrayList<HopUnderReplicatedBlock>();
    for (UnderReplicatedBlocksDTO bit : bitList) {
      blocks.add(createUrBlock(bit));
    }
    return blocks;
  }

  @Override
  public int countAll() throws StorageException {
    return MySQLQueryHelper.countAll(TABLE_NAME);
  }

  @Override
  public List<HopUnderReplicatedBlock> findAll() throws StorageException {
    try {
      DBSession dbSession = connector.obtainSession();
      QueryBuilder qb = dbSession.getSession().getQueryBuilder();
      QueryDomainType<UnderReplicatedBlocksDTO> dobj = qb.createQueryDefinition(UnderReplicatedBlocksDTO.class);
      Query<UnderReplicatedBlocksDTO> query = dbSession.getSession().createQuery(dobj);
      query.setOrdering(Query.Ordering.ASCENDING, "level", "timestamp");
      List<HopUnderReplicatedBlock> blocks = createUrBlockList(query.getResultList());
      return blocks;
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopUnderReplicatedBlock> findByLevel(int level) throws StorageException {
     try {
      DBSession dbSession = connector.obtainSession();
      QueryBuilder qb = dbSession.getSession().getQueryBuilder();
      QueryDomainType<UnderReplicatedBlocksDTO> dobj = qb.createQueryDefinition(UnderReplicatedBlocksDTO.class);
      Predicate pred = dobj.get("level").equal(dobj.param("level"));
      dobj.where(pred);
      Query<UnderReplicatedBlocksDTO> query = dbSession.getSession().createQuery(dobj);
      query.setParameter("level", level);
      query.setOrdering(Query.Ordering.ASCENDING,"level","timestamp");
      return createUrBlockList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopUnderReplicatedBlock> findByLevel(int level, int offset, int count) throws StorageException {
    try {
      DBSession dbSession = connector.obtainSession();
      QueryBuilder qb = dbSession.getSession().getQueryBuilder();
      QueryDomainType<UnderReplicatedBlocksDTO> dobj = qb.createQueryDefinition(UnderReplicatedBlocksDTO.class);
      Predicate pred = dobj.get("level").equal(dobj.param("level"));
      dobj.where(pred);
      Query<UnderReplicatedBlocksDTO> query = dbSession.getSession().createQuery(dobj);
      query.setParameter("level", level);
      query.setOrdering(Query.Ordering.ASCENDING, "level", "timestamp");
      query.setLimits(offset, count);
      return createUrBlockList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }
    
  @Override
  public List<HopUnderReplicatedBlock> findByINodeId(int inodeId) throws StorageException {
    try {
      DBSession dbSession = connector.obtainSession();
      QueryBuilder qb = dbSession.getSession().getQueryBuilder();
      QueryDomainType<UnderReplicatedBlocksDTO> qdt = qb.createQueryDefinition(UnderReplicatedBlocksDTO.class);
      Predicate pred1 = qdt.get("iNodeId").equal(qdt.param("idParam"));
      qdt.where(pred1);
      Query<UnderReplicatedBlocksDTO> query = dbSession.getSession().createQuery(qdt);
      query.setParameter("idParam", inodeId);
      //FIXME[M]: it throws ClusterJUserException: There is no index containing the ordering fields.
      //http://bugs.mysql.com/bug.php?id=67765
      //query.setOrdering(Query.Ordering.ASCENDING, "level", "timestamp");
      return createUrBlockList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopUnderReplicatedBlock> findByINodeIds(int[] inodeIds) throws StorageException {
    try {
      DBSession dbSession = connector.obtainSession();
      QueryBuilder qb = dbSession.getSession().getQueryBuilder();
      QueryDomainType<UnderReplicatedBlocksDTO> qdt = qb.createQueryDefinition(UnderReplicatedBlocksDTO.class);
      Predicate pred1 = qdt.get("iNodeId").in(qdt.param("idParam"));
      qdt.where(pred1);
      Query<UnderReplicatedBlocksDTO> query = dbSession.getSession().createQuery(qdt);
      query.setParameter("idParam", Ints.asList(inodeIds));
      return createUrBlockList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }
  
  @Override
  public void removeAll() throws StorageException {
    DBSession dbSession = connector.obtainSession();
    dbSession.getSession().deletePersistentAll(UnderReplicatedBlocksDTO.class);
  }
}
