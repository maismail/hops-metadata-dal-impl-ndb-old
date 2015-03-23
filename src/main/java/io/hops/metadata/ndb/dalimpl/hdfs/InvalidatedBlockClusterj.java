package io.hops.metadata.ndb.dalimpl.hdfs;

import com.google.common.primitives.Ints;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.dal.InvalidateBlockDataAccess;
import io.hops.metadata.hdfs.entity.InvalidatedBlock;
import io.hops.metadata.hdfs.tabledef.InvalidatedBlockTableDef;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class InvalidatedBlockClusterj implements InvalidatedBlockTableDef,
    InvalidateBlockDataAccess<InvalidatedBlock> {

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
  public List<InvalidatedBlock> findAllInvalidatedBlocks() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType qdt = qb.createQueryDefinition(InvalidateBlocksDTO.class);
    return createList(session.createQuery(qdt).getResultList());
  }

  @Override
  public List<InvalidatedBlock> findInvalidatedBlockByStorageId(int storageId) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<InvalidateBlocksDTO> qdt = qb.createQueryDefinition(InvalidateBlocksDTO.class);
    qdt.where(qdt.get("storageId").equal(qdt.param("param")));
    HopsQuery<InvalidateBlocksDTO> query = session.createQuery(qdt);
    query.setParameter("param", storageId);
    return createList(query.getResultList());
  }

  @Override
  public List<InvalidatedBlock> findInvalidatedBlocksByBlockId(long bid, int inodeId) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<InvalidateBlocksDTO> qdt = qb.createQueryDefinition(InvalidateBlocksDTO.class);
    HopsPredicate pred1 = qdt.get("blockId").equal(qdt.param("blockIdParam"));
    HopsPredicate pred2 = qdt.get("iNodeId").equal(qdt.param("iNodeIdParam"));
    qdt.where(pred1.and(pred2));
    HopsQuery<InvalidateBlocksDTO> query = session.createQuery(qdt);
    query.setParameter("blockIdParam", bid);
    query.setParameter("iNodeIdParam", inodeId);
    return createList(query.getResultList());
  }
  
  @Override
  public List<InvalidatedBlock> findInvalidatedBlocksByINodeId(int inodeId) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<InvalidateBlocksDTO> qdt = qb.createQueryDefinition(InvalidateBlocksDTO.class);
    HopsPredicate pred1 = qdt.get("iNodeId").equal(qdt.param("iNodeIdParam"));
    qdt.where(pred1);
    HopsQuery<InvalidateBlocksDTO> query = session.createQuery(qdt);
    query.setParameter("iNodeIdParam", inodeId);
    return createList(query.getResultList());
  }

  @Override
  public List<InvalidatedBlock> findInvalidatedBlocksByINodeIds(int[] inodeIds) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<InvalidateBlocksDTO> qdt = qb.createQueryDefinition(InvalidateBlocksDTO.class);
    HopsPredicate pred1 = qdt.get("iNodeId").in(qdt.param("iNodeIdParam"));
    qdt.where(pred1);
    HopsQuery<InvalidateBlocksDTO> query = session.createQuery(qdt);
    query.setParameter("iNodeIdParam", Ints.asList(inodeIds));
    return createList(query.getResultList());
  }
  
  @Override
  public InvalidatedBlock findInvBlockByPkey(long blockId, int storageId, int inodeId) throws StorageException {
    HopsSession session = connector.obtainSession();
    Object[] pk = new Object[3];
    pk[0] = inodeId;
    pk[1] = blockId;
    pk[2] = storageId;

    InvalidateBlocksDTO invTable = session.find(InvalidateBlocksDTO.class, pk);
    if (invTable == null) {
      return null;
    }
    return createReplica(invTable);
  }

  @Override
  public List<InvalidatedBlock> findInvalidatedBlocksbyPKS(final long[] blockIds, final int[] inodesIds, final int[] storageIds) throws StorageException {
    int currentTableSize = countAll();
    if(currentTableSize == 0){
      return new ArrayList<InvalidatedBlock>();
    }else if(currentTableSize < inodesIds.length){
      return findAllInvalidatedBlocks();
    }
    final List<InvalidateBlocksDTO> invBlocks = new ArrayList<InvalidateBlocksDTO>();
    HopsSession session = connector.obtainSession();
    for (int i = 0; i < blockIds.length; i++) {
      InvalidateBlocksDTO invTable = session.newInstance(
          InvalidateBlocksDTO.class,
          new Object[]{inodesIds[i], blockIds[i], storageIds[i]});
      invTable.setGenerationStamp(NOT_FOUND_ROW);
      invTable = session.load(invTable);
      invBlocks.add(invTable);
    }
    session.flush();
    return createList(invBlocks);
  }

  @Override
  public void prepare(Collection<InvalidatedBlock> removed, Collection<InvalidatedBlock> newed, Collection<InvalidatedBlock> modified) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<InvalidateBlocksDTO> changes = new ArrayList<InvalidateBlocksDTO>();
    List<InvalidateBlocksDTO> deletions = new ArrayList<InvalidateBlocksDTO>();
    for (InvalidatedBlock invBlock : newed) {
      InvalidateBlocksDTO newInstance = session.newInstance(
          InvalidateBlocksDTO.class);
      createPersistable(invBlock, newInstance);
      changes.add(newInstance );
    }

    for (InvalidatedBlock invBlock : removed) {
      InvalidateBlocksDTO newInstance = session.newInstance(
          InvalidateBlocksDTO.class);
      createPersistable(invBlock, newInstance);
      deletions.add(newInstance);
    }

    if (!modified.isEmpty()) {
      throw new UnsupportedOperationException("Not yet Implemented");
    }
    session.deletePersistentAll(deletions);
    session.savePersistentAll(changes);
  }

  @Override
  public void removeAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    session.deletePersistentAll(InvalidateBlocksDTO.class);
  }

  @Override
  public void removeAllByStorageId(int storageId) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<InvalidateBlocksDTO> qdt = qb.createQueryDefinition(InvalidateBlocksDTO.class);
    qdt.where(qdt.get("storageId").equal(qdt.param("param")));
    HopsQuery<InvalidateBlocksDTO> query = session.createQuery(qdt);
    query.setParameter("param", storageId);
    query.deletePersistentAll();
  }


  private List<InvalidatedBlock> createList(List<InvalidateBlocksDTO> dtoList) {
    List<InvalidatedBlock> list = new ArrayList<InvalidatedBlock>();
    for (InvalidateBlocksDTO dto : dtoList) {
      if (dto.getGenerationStamp() != NOT_FOUND_ROW) {
        list.add(createReplica(dto));
      }
    }
    return list;
  }

  private InvalidatedBlock createReplica(InvalidateBlocksDTO invBlockTable) {
    return new InvalidatedBlock(invBlockTable.getStorageId(), invBlockTable.getBlockId(),
        invBlockTable.getGenerationStamp(), invBlockTable.getNumBytes(),invBlockTable.getINodeId());
  }

  private void createPersistable(InvalidatedBlock invBlock, InvalidateBlocksDTO newInvTable) {
    newInvTable.setBlockId(invBlock.getBlockId());
    newInvTable.setStorageId(invBlock.getStorageId());
    newInvTable.setGenerationStamp(invBlock.getGenerationStamp());
    newInvTable.setNumBytes(invBlock.getNumBytes());
    newInvTable.setINodeId(invBlock.getInodeId());
  }
}
