package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.google.common.primitives.Ints;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import se.sics.hop.metadata.hdfs.dal.BlockInfoDataAccess;
import se.sics.hop.metadata.hdfs.entity.hdfs.HopBlockInfo;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.hop.HopBlockLookUp;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.mysqlserver.CountHelper;
import se.sics.hop.metadata.hdfs.tabledef.BlockInfoTableDef;
import se.sics.hop.util.Slicer;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class BlockInfoClusterj implements BlockInfoTableDef, BlockInfoDataAccess<HopBlockInfo> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface BlockInfoDTO {

    @PrimaryKey
    @Column(name = INODE_ID)
    int getINodeId();

    void setINodeId(int iNodeID);

    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();

    void setBlockId(long bid);

    @Column(name = BLOCK_INDEX)
    int getBlockIndex();

    void setBlockIndex(int idx);

    @Column(name = NUM_BYTES)
    long getNumBytes();

    void setNumBytes(long numbytes);

    @Column(name = GENERATION_STAMP)
    long getGenerationStamp();

    void setGenerationStamp(long genstamp);

    @Column(name = BLOCK_UNDER_CONSTRUCTION_STATE)
    int getBlockUCState();

    void setBlockUCState(int BlockUCState);

    @Column(name = TIME_STAMP)
    long getTimestamp();

    void setTimestamp(long ts);

    @Column(name = PRIMARY_NODE_INDEX)
    int getPrimaryNodeIndex();

    void setPrimaryNodeIndex(int replication);

    @Column(name = BLOCK_RECOVERY_ID)
    long getBlockRecoveryId();

    void setBlockRecoveryId(long recoveryId);
  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();
  private final static int NOT_FOUND_ROW = -1000;

  @Override
  public int countAll() throws StorageException {
    return CountHelper.countAll(TABLE_NAME);
  }

  @Override
  public int countAllCompleteBlocks() throws StorageException {
    return CountHelper.countWithCriterion(TABLE_NAME, String.format("%s=%d", BLOCK_UNDER_CONSTRUCTION_STATE, 0));
  }
  
  @Override
  public void prepare(Collection<HopBlockInfo> removed, Collection<HopBlockInfo> news, Collection<HopBlockInfo> modified) throws StorageException {
    try {
      List<BlockInfoDTO> blkChanges = new ArrayList<BlockInfoDTO>();
      List<BlockInfoDTO> blkDeletions = new ArrayList<BlockInfoDTO>();
      List<BlockLookUpClusterj.BlockLookUpDTO> luChanges = new ArrayList<BlockLookUpClusterj.BlockLookUpDTO>();
      List<BlockLookUpClusterj.BlockLookUpDTO> luDeletions = new ArrayList<BlockLookUpClusterj.BlockLookUpDTO>();
      Session session = connector.obtainSession();
      for (HopBlockInfo block : removed) {
        Object[] pk = new Object[2];
        pk[0] = block.getInodeId();
        pk[1] = block.getBlockId();

        BlockInfoClusterj.BlockInfoDTO bTable = session.newInstance(BlockInfoClusterj.BlockInfoDTO.class, pk);
        blkDeletions.add(bTable);

        //delete the row from persistance table 
        BlockLookUpClusterj.BlockLookUpDTO lookupDTO = session.newInstance(BlockLookUpClusterj.BlockLookUpDTO.class, block.getBlockId());
        luDeletions.add(lookupDTO);
      }

      for (HopBlockInfo block : news) {
        BlockInfoClusterj.BlockInfoDTO bTable = session.newInstance(BlockInfoClusterj.BlockInfoDTO.class);
        createPersistable(block, bTable);
        blkChanges.add(bTable);

        //save a new row in the lookup table
        BlockLookUpClusterj.BlockLookUpDTO lookupDTO = session.newInstance(BlockLookUpClusterj.BlockLookUpDTO.class);
        BlockLookUpClusterj.createPersistable(new HopBlockLookUp(block.getBlockId(), block.getInodeId()), lookupDTO);
        luChanges.add(lookupDTO);
      }

      for (HopBlockInfo block : modified) {
        BlockInfoClusterj.BlockInfoDTO bTable = session.newInstance(BlockInfoClusterj.BlockInfoDTO.class);
        createPersistable(block, bTable);
        blkChanges.add(bTable);

        //save a new row in the lookup table
        BlockLookUpClusterj.BlockLookUpDTO lookupDTO = session.newInstance(BlockLookUpClusterj.BlockLookUpDTO.class);
        BlockLookUpClusterj.createPersistable(new HopBlockLookUp(block.getBlockId(), block.getInodeId()), lookupDTO);
        luChanges.add(lookupDTO);
      }
      session.deletePersistentAll(blkDeletions);
      session.deletePersistentAll(luDeletions);
      session.savePersistentAll(blkChanges);
      session.savePersistentAll(luChanges);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public HopBlockInfo findById(long blockId, int inodeId) throws StorageException {
    try {
      Object[] pk = new Object[2];
      pk[0] = inodeId;
      pk[1] = blockId;

      Session session = connector.obtainSession();
      BlockInfoClusterj.BlockInfoDTO bit = session.find(BlockInfoClusterj.BlockInfoDTO.class, pk);
      if (bit == null) {
        return null;
      }
      return createBlockInfo(bit);
    } catch (Exception e) {
        throw new StorageException(e);
      }
  }

  @Override
  public List<HopBlockInfo> findByInodeId(int inodeId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<BlockInfoClusterj.BlockInfoDTO> dobj = qb.createQueryDefinition(BlockInfoClusterj.BlockInfoDTO.class);
      Predicate pred1 = dobj.get("iNodeId").equal(dobj.param("iNodeParam"));
      dobj.where(pred1);
      Query<BlockInfoClusterj.BlockInfoDTO> query = session.createQuery(dobj);
      query.setParameter("iNodeParam", inodeId);
      return createBlockInfoList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopBlockInfo> findByInodeIds(int[] inodeIds) throws StorageException {
     try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<BlockInfoClusterj.BlockInfoDTO> dobj = qb.createQueryDefinition(BlockInfoClusterj.BlockInfoDTO.class);
      Predicate pred1 = dobj.get("iNodeId").in(dobj.param("iNodeParam"));
      dobj.where(pred1);
      Query<BlockInfoClusterj.BlockInfoDTO> query = session.createQuery(dobj);
      query.setParameter("iNodeParam", Ints.asList(inodeIds));
      return createBlockInfoList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }
    
  public HopBlockInfo scanByBlockId(long blockId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<BlockInfoClusterj.BlockInfoDTO> dobj = qb.createQueryDefinition(BlockInfoClusterj.BlockInfoDTO.class);
      Predicate pred1 = dobj.get("blockId").equal(dobj.param("blockIdParam"));
      dobj.where(pred1);
      Query<BlockInfoClusterj.BlockInfoDTO> query = session.createQuery(dobj);
      query.setParameter("blockIdParam", blockId);
      return createBlockInfo(query.getResultList().get(0));
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopBlockInfo> findAllBlocks() throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<BlockInfoClusterj.BlockInfoDTO> dobj = qb.createQueryDefinition(BlockInfoClusterj.BlockInfoDTO.class);
      Query<BlockInfoClusterj.BlockInfoDTO> query = session.createQuery(dobj);
      return createBlockInfoList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopBlockInfo> findByStorageId(int storageId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      session.currentTransaction().begin();
      List<ReplicaClusterj.ReplicaDTO> replicas = getReplicas(session, storageId);
      long[] blockIds = new long[replicas.size()];
      for (int i = 0; i < blockIds.length; i++) {
        blockIds[i] = replicas.get(i).getBlockId();
      }

      List<HopBlockInfo> ret = readBlockInfoBatch(session, blockIds, false);
      session.currentTransaction().commit();
      return ret;
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopBlockInfo> findByIds(long[] blockIds, int[] inodeIds) throws StorageException {
    try {
      Session session = connector.obtainSession();
      return readBlockInfoBatch(session, inodeIds, blockIds, true);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopBlockInfo> findByIdsNoCommit(long[] blockIds, int[] inodeIds) throws StorageException {
    try {
      Session session = connector.obtainSession();
      return readBlockInfoBatch(session, inodeIds, blockIds, false);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public Set<Long> findByStorageIdOnlyIds(int storageId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      List<ReplicaClusterj.ReplicaDTO> replicas = getReplicas(session, storageId);
      Set<Long> blockIds = new HashSet<Long>();
      for (ReplicaClusterj.ReplicaDTO r : replicas) {
        blockIds.add(r.getBlockId());
      }
      return blockIds;
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private List<ReplicaClusterj.ReplicaDTO> getReplicas(Session session, int storageId) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<ReplicaClusterj.ReplicaDTO> dobj = qb.createQueryDefinition(ReplicaClusterj.ReplicaDTO.class);
    dobj.where(dobj.get("storageId").equal(dobj.param("param")));
    Query<ReplicaClusterj.ReplicaDTO> query = session.createQuery(dobj);
    query.setParameter("param", storageId);
    return query.getResultList();
  }

  private List<HopBlockInfo> readBlockInfoBatch(final Session session, final long[] blockIds, final boolean commitAfterFlush) throws Exception {
    int[] inodeIds = BlockLookUpClusterj.readINodeIdsByBlockIds(session, blockIds);
    return readBlockInfoBatch(session, inodeIds, blockIds, commitAfterFlush);
  }
  
  private List<HopBlockInfo> readBlockInfoBatch(final Session session, final int[] inodeIds, final long[] blockIds, final boolean commitAfterFlush) throws Exception {
    final List<BlockInfoClusterj.BlockInfoDTO> bdtos = new ArrayList<BlockInfoDTO>();
    Slicer.slice(blockIds.length, connector.getBatchSize(), new Slicer.OperationHandler() {
      @Override
      public void handle(int startIndex, int endIndex) throws Exception {
        for (int i = startIndex; i < endIndex; i++) {
          Object[] pk = new Object[]{inodeIds[i], blockIds[i]};
          BlockInfoClusterj.BlockInfoDTO bdto = session.newInstance(BlockInfoClusterj.BlockInfoDTO.class, pk);
          bdto.setBlockIndex(NOT_FOUND_ROW);
          bdto = session.load(bdto);
          bdtos.add(bdto);
        }
        session.flush();
        if (commitAfterFlush) {
          session.currentTransaction().commit();
          session.currentTransaction().begin();
        }
      }
    });

    return createBlockInfoList(bdtos);
  }
  
  private List<HopBlockInfo> createBlockInfoList(List<BlockInfoClusterj.BlockInfoDTO> bitList) {
    List<HopBlockInfo> list = new ArrayList<HopBlockInfo>();

    for (BlockInfoClusterj.BlockInfoDTO blockInfoDTO : bitList) {
      if (blockInfoDTO.getBlockIndex() != NOT_FOUND_ROW) {
        list.add(createBlockInfo(blockInfoDTO));
      }
    }

    return list;
  }

  private HopBlockInfo createBlockInfo(BlockInfoClusterj.BlockInfoDTO bDTO) {
    HopBlockInfo hopBlockInfo = new HopBlockInfo(
            bDTO.getBlockId(),
            bDTO.getBlockIndex(),
            bDTO.getINodeId(),
            bDTO.getNumBytes(),
            bDTO.getGenerationStamp(),
            bDTO.getBlockUCState(),
            bDTO.getTimestamp(),
            bDTO.getPrimaryNodeIndex(),
            bDTO.getBlockRecoveryId());
    return hopBlockInfo;
  }

  private void createPersistable(HopBlockInfo block, BlockInfoClusterj.BlockInfoDTO persistable) {
    persistable.setBlockId(block.getBlockId());
    persistable.setNumBytes(block.getNumBytes());
    persistable.setGenerationStamp(block.getGenerationStamp());
    persistable.setINodeId(block.getInodeId());
    persistable.setTimestamp(block.getTimeStamp());
    persistable.setBlockIndex(block.getBlockIndex());
    persistable.setBlockUCState(block.getBlockUCState());
    persistable.setPrimaryNodeIndex(block.getPrimaryNodeIndex());
    persistable.setBlockRecoveryId(block.getBlockRecoveryId());
  }
}
