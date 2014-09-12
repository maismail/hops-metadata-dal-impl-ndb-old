package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.google.common.primitives.Ints;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
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
import se.sics.hop.metadata.ndb.mysqlserver.CountHelper;
import se.sics.hop.metadata.hdfs.tabledef.InvalidatedBlockTableDef;
import se.sics.hop.util.Slicer;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class InvalidatedBlockClusterj implements InvalidatedBlockTableDef, InvalidateBlockDataAccess<HopInvalidatedBlock> {

  @PersistenceCapable(table = TABLE_NAME)
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
    @Index(name = STORAGE_IDX)
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
    return CountHelper.countAll(TABLE_NAME);
  }

  @Override
  public List<HopInvalidatedBlock> findAllInvalidatedBlocks() throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType qdt = qb.createQueryDefinition(InvalidateBlocksDTO.class);
      return createList(session.createQuery(qdt).getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopInvalidatedBlock> findInvalidatedBlockByStorageId(int storageId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<InvalidateBlocksDTO> qdt = qb.createQueryDefinition(InvalidateBlocksDTO.class);
      qdt.where(qdt.get("storageId").equal(qdt.param("param")));
      Query<InvalidateBlocksDTO> query = session.createQuery(qdt);
      query.setParameter("param", storageId);
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopInvalidatedBlock> findInvalidatedBlocksByBlockId(long bid, int inodeId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<InvalidateBlocksDTO> qdt = qb.createQueryDefinition(InvalidateBlocksDTO.class);
      Predicate pred1 = qdt.get("blockId").equal(qdt.param("blockIdParam"));
      Predicate pred2 = qdt.get("iNodeId").equal(qdt.param("iNodeIdParam"));
      qdt.where(pred1.and(pred2));
      Query<InvalidateBlocksDTO> query = session.createQuery(qdt);
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
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<InvalidateBlocksDTO> qdt = qb.createQueryDefinition(InvalidateBlocksDTO.class);
      Predicate pred1 = qdt.get("iNodeId").equal(qdt.param("iNodeIdParam"));
      qdt.where(pred1);
      Query<InvalidateBlocksDTO> query = session.createQuery(qdt);
      query.setParameter("iNodeIdParam", inodeId);
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopInvalidatedBlock> findInvalidatedBlocksByINodeIds(int[] inodeIds) throws StorageException {
       try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<InvalidateBlocksDTO> qdt = qb.createQueryDefinition(InvalidateBlocksDTO.class);
      Predicate pred1 = qdt.get("iNodeId").in(qdt.param("iNodeIdParam"));
      qdt.where(pred1);
      Query<InvalidateBlocksDTO> query = session.createQuery(qdt);
      query.setParameter("iNodeIdParam", Ints.asList(inodeIds));
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }
  
  @Override
  public HopInvalidatedBlock findInvBlockByPkey(long blockId, int storageId, int inodeId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      Object[] pk = new Object[3];
      pk[0] = inodeId;
      pk[1] = blockId;
      pk[2] = storageId;
              
      InvalidateBlocksDTO invTable = session.find(InvalidateBlocksDTO.class, pk);
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
      final List<InvalidateBlocksDTO> invBlocks = new ArrayList<InvalidateBlocksDTO>();

      Slicer.slice(blockIds.length, connector.getBatchSize(), new Slicer.OperationHandler() {
        @Override
        public void handle(int startIndex, int endIndex) throws Exception {
          Session session = connector.obtainSession();
          for (int i = startIndex; i < endIndex; i++) {
            InvalidateBlocksDTO invTable = session.newInstance(InvalidateBlocksDTO.class, new Object[]{inodesIds[i], blockIds[i], storageIds[i]});
            invTable.setGenerationStamp(NOT_FOUND_ROW);
            invTable = session.load(invTable);
            invBlocks.add(invTable);
          }
          session.flush();

          session.currentTransaction().commit();
          session.currentTransaction().begin();
        }
      });

      return createList(invBlocks);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void prepare(Collection<HopInvalidatedBlock> removed, Collection<HopInvalidatedBlock> newed, Collection<HopInvalidatedBlock> modified) throws StorageException {
    try {
      Session session = connector.obtainSession();
      List<InvalidateBlocksDTO> changes = new ArrayList<InvalidateBlocksDTO>();
      List<InvalidateBlocksDTO> deletions = new ArrayList<InvalidateBlocksDTO>();
      for (HopInvalidatedBlock invBlock : newed) {
        InvalidateBlocksDTO newInstance = session.newInstance(InvalidateBlocksDTO.class);
        createPersistable(invBlock, newInstance);
        changes.add(newInstance );
      }

      for (HopInvalidatedBlock invBlock : removed) {
        InvalidateBlocksDTO newInstance = session.newInstance(InvalidateBlocksDTO.class);
        createPersistable(invBlock, newInstance);
        deletions.add(newInstance);
      }

      if (!modified.isEmpty()) {
        throw new UnsupportedOperationException("Not yet Implemented");
      }
      session.deletePersistentAll(deletions);
      session.savePersistentAll(changes);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void removeAll() throws StorageException {
    try {
      Session session = connector.obtainSession();
      session.deletePersistentAll(InvalidateBlocksDTO.class);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void removeAllByStorageId(int storageId) throws StorageException {
    try {
    Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<InvalidateBlocksDTO> qdt = qb.createQueryDefinition(InvalidateBlocksDTO.class);
      qdt.where(qdt.get("storageId").equal(qdt.param("param")));
      Query<InvalidateBlocksDTO> query = session.createQuery(qdt);
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
