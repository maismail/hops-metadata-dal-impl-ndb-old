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
import se.sics.hop.metadata.hdfs.entity.hop.HopInvalidatedBlock;
import se.sics.hop.metadata.hdfs.dal.InvalidateBlockDataAccess;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.mysqlserver.MySQLQueryHelper;
import se.sics.hop.metadata.hdfs.tabledef.InvalidatedBlockTableDef;
import se.sics.hop.metadata.ndb.DBSession;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class InvalidatedBlockClusterj implements InvalidatedBlockTableDef, InvalidateBlockDataAccess<HopInvalidatedBlock> {

  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column=INODE_ID)
  @Index(name = STORAGE_IDX)
  public interface InvalidateBlocksDTO {

    @PrimaryKey
    @Column(name = INODE_ID)
    int getINodeId();
    void setINodeId(int inodeID);
    
    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();
    void setBlockId(long blockId);
    
    @PrimaryKey
    @Column(name = STORAGE_ID)
    int getStorageId();
    void setStorageId(int storageId);
    
    @Column(name = GENERATION_STAMP)
    long getGenerationStamp();
    void setGenerationStamp(long generationStamp);

    @Column(name = NUM_BYTES)
    long getNumBytes();
    void setNumBytes(long numBytes);
  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();
  private final static int NOT_FOUND_ROW = -1000;
  
  @Override
  public int countAll() throws StorageException {
    return MySQLQueryHelper.countAll(TABLE_NAME);
  }

  @Override
  public List<HopInvalidatedBlock> findAllInvalidatedBlocks() throws StorageException {
    try {
      DBSession dbSession = connector.obtainSession();
      QueryBuilder qb = dbSession.getSession().getQueryBuilder();
      QueryDomainType qdt = qb.createQueryDefinition(InvalidateBlocksDTO.class);
      return createList(dbSession.getSession().createQuery(qdt).getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopInvalidatedBlock> findInvalidatedBlockByStorageId(int storageId) throws StorageException {
    try {
      DBSession dbSession = connector.obtainSession();
      QueryBuilder qb = dbSession.getSession().getQueryBuilder();
      QueryDomainType<InvalidateBlocksDTO> qdt = qb.createQueryDefinition(InvalidateBlocksDTO.class);
      qdt.where(qdt.get("storageId").equal(qdt.param("param")));
      Query<InvalidateBlocksDTO> query = dbSession.getSession().createQuery(qdt);
      query.setParameter("param", storageId);
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopInvalidatedBlock> findInvalidatedBlocksByBlockId(long bid, int inodeId) throws StorageException {
    try {
      DBSession dbSession = connector.obtainSession();
      QueryBuilder qb = dbSession.getSession().getQueryBuilder();
      QueryDomainType<InvalidateBlocksDTO> qdt = qb.createQueryDefinition(InvalidateBlocksDTO.class);
      Predicate pred1 = qdt.get("blockId").equal(qdt.param("blockIdParam"));
      Predicate pred2 = qdt.get("iNodeId").equal(qdt.param("iNodeIdParam"));
      qdt.where(pred1.and(pred2));
      Query<InvalidateBlocksDTO> query = dbSession.getSession().createQuery(qdt);
      query.setParameter("blockIdParam", bid);
      query.setParameter("iNodeIdParam", inodeId);
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }
  
  @Override
  public List<HopInvalidatedBlock> findInvalidatedBlocksByINodeId(int inodeId) throws StorageException {
    try {
      DBSession dbSession = connector.obtainSession();
      QueryBuilder qb = dbSession.getSession().getQueryBuilder();
      QueryDomainType<InvalidateBlocksDTO> qdt = qb.createQueryDefinition(InvalidateBlocksDTO.class);
      Predicate pred1 = qdt.get("iNodeId").equal(qdt.param("iNodeIdParam"));
      qdt.where(pred1);
      Query<InvalidateBlocksDTO> query = dbSession.getSession().createQuery(qdt);
      query.setParameter("iNodeIdParam", inodeId);
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopInvalidatedBlock> findInvalidatedBlocksByINodeIds(int[] inodeIds) throws StorageException {
       try {
      DBSession dbSession = connector.obtainSession();
      QueryBuilder qb = dbSession.getSession().getQueryBuilder();
      QueryDomainType<InvalidateBlocksDTO> qdt = qb.createQueryDefinition(InvalidateBlocksDTO.class);
      Predicate pred1 = qdt.get("iNodeId").in(qdt.param("iNodeIdParam"));
      qdt.where(pred1);
      Query<InvalidateBlocksDTO> query = dbSession.getSession().createQuery(qdt);
      query.setParameter("iNodeIdParam", Ints.asList(inodeIds));
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }
  
  @Override
  public HopInvalidatedBlock findInvBlockByPkey(long blockId, int storageId, int inodeId) throws StorageException {
    try {
      DBSession dbSession = connector.obtainSession();
      Object[] pk = new Object[3];
      pk[0] = inodeId;
      pk[1] = blockId;
      pk[2] = storageId;
              
      InvalidateBlocksDTO invTable = dbSession.getSession().find(InvalidateBlocksDTO.class, pk);
      if (invTable == null) {
        return null;
      }
      return createReplica(invTable);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopInvalidatedBlock> findInvalidatedBlocksbyPKS(final long[] blockIds, final int[] inodesIds, final int[] storageIds) throws StorageException {
    try {
      int currentTableSize = countAll();
      if(currentTableSize == 0){
        return new ArrayList<HopInvalidatedBlock>();
      }else if(currentTableSize < inodesIds.length){
        return findAllInvalidatedBlocks();
      }
      final List<InvalidateBlocksDTO> invBlocks = new ArrayList<InvalidateBlocksDTO>();
      DBSession dbSession = connector.obtainSession();
      for (int i = 0; i < blockIds.length; i++) {
        InvalidateBlocksDTO invTable = dbSession.getSession().newInstance(InvalidateBlocksDTO.class, new Object[]{inodesIds[i], blockIds[i], storageIds[i]});
        invTable.setGenerationStamp(NOT_FOUND_ROW);
        invTable = dbSession.getSession().load(invTable);
        invBlocks.add(invTable);
      }
      dbSession.getSession().flush();
      return createList(invBlocks);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void prepare(Collection<HopInvalidatedBlock> removed, Collection<HopInvalidatedBlock> newed, Collection<HopInvalidatedBlock> modified) throws StorageException {
    try {
      DBSession dbSession = connector.obtainSession();
      List<InvalidateBlocksDTO> changes = new ArrayList<InvalidateBlocksDTO>();
      List<InvalidateBlocksDTO> deletions = new ArrayList<InvalidateBlocksDTO>();
      for (HopInvalidatedBlock invBlock : newed) {
        InvalidateBlocksDTO newInstance = dbSession.getSession().newInstance(InvalidateBlocksDTO.class);
        createPersistable(invBlock, newInstance);
        changes.add(newInstance );
      }

      for (HopInvalidatedBlock invBlock : removed) {
        InvalidateBlocksDTO newInstance = dbSession.getSession().newInstance(InvalidateBlocksDTO.class);
        createPersistable(invBlock, newInstance);
        deletions.add(newInstance);
      }

      if (!modified.isEmpty()) {
        throw new UnsupportedOperationException("Not yet Implemented");
      }
      dbSession.getSession().deletePersistentAll(deletions);
      dbSession.getSession().savePersistentAll(changes);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void removeAll() throws StorageException {
    try {
      DBSession dbSession = connector.obtainSession();
      dbSession.getSession().deletePersistentAll(InvalidateBlocksDTO.class);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void removeAllByStorageId(int storageId) throws StorageException {
    try {
    DBSession dbSession = connector.obtainSession();
      QueryBuilder qb = dbSession.getSession().getQueryBuilder();
      QueryDomainType<InvalidateBlocksDTO> qdt = qb.createQueryDefinition(InvalidateBlocksDTO.class);
      qdt.where(qdt.get("storageId").equal(qdt.param("param")));
      Query<InvalidateBlocksDTO> query = dbSession.getSession().createQuery(qdt);
      query.setParameter("param", storageId);
      query.deletePersistentAll();
    } catch (Exception e) {
      throw new StorageException(e);
  }
  }


  private List<HopInvalidatedBlock> createList(List<InvalidateBlocksDTO> dtoList) {
    List<HopInvalidatedBlock> list = new ArrayList<HopInvalidatedBlock>();
    for (InvalidateBlocksDTO dto : dtoList) {
      if (dto.getGenerationStamp() != NOT_FOUND_ROW) {
        list.add(createReplica(dto));
      }
    }
    return list;
  }

  private HopInvalidatedBlock createReplica(InvalidateBlocksDTO invBlockTable) {
    return new HopInvalidatedBlock(invBlockTable.getStorageId(), invBlockTable.getBlockId(),
            invBlockTable.getGenerationStamp(), invBlockTable.getNumBytes(),invBlockTable.getINodeId());
  }

  private void createPersistable(HopInvalidatedBlock invBlock, InvalidateBlocksDTO newInvTable) {
    newInvTable.setBlockId(invBlock.getBlockId());
    newInvTable.setStorageId(invBlock.getStorageId());
    newInvTable.setGenerationStamp(invBlock.getGenerationStamp());
    newInvTable.setNumBytes(invBlock.getNumBytes());
    newInvTable.setINodeId(invBlock.getInodeId());
  }
}
