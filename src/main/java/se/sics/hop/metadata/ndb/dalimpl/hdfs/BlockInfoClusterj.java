package se.sics.hop.metadata.ndb.dalimpl.hdfs;

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
import se.sics.hop.metadata.hdfs.dal.BlockInfoDataAccess;
import se.sics.hop.metadata.hdfs.entity.hdfs.HopBlockInfo;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.hop.HopBlockLookUp;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.mysqlserver.CountHelper;
import se.sics.hop.metadata.hdfs.tabledef.BlockInfoTableDef;
import se.sics.hop.metadata.ndb.DBSession;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class BlockInfoClusterj implements BlockInfoTableDef, BlockInfoDataAccess<HopBlockInfo> {

  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column = INODE_ID)
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
      DBSession dbSession = connector.obtainSession();
      for (HopBlockInfo block : removed) {
        Object[] pk = new Object[2];
        pk[0] = block.getInodeId();
        pk[1] = block.getBlockId();

        BlockInfoClusterj.BlockInfoDTO bTable = dbSession.getSession().newInstance(BlockInfoClusterj.BlockInfoDTO.class, pk);
        blkDeletions.add(bTable);

        //delete the row from persistance table 
        BlockLookUpClusterj.BlockLookUpDTO lookupDTO = dbSession.getSession().newInstance(BlockLookUpClusterj.BlockLookUpDTO.class, block.getBlockId());
        luDeletions.add(lookupDTO);
      }

      for (HopBlockInfo block : news) {
        BlockInfoClusterj.BlockInfoDTO bTable = dbSession.getSession().newInstance(BlockInfoClusterj.BlockInfoDTO.class);
        createPersistable(block, bTable);
        blkChanges.add(bTable);

        //save a new row in the lookup table
        BlockLookUpClusterj.BlockLookUpDTO lookupDTO = dbSession.getSession().newInstance(BlockLookUpClusterj.BlockLookUpDTO.class);
        BlockLookUpClusterj.createPersistable(new HopBlockLookUp(block.getBlockId(), block.getInodeId()), lookupDTO);
        luChanges.add(lookupDTO);
      }

      for (HopBlockInfo block : modified) {
        BlockInfoClusterj.BlockInfoDTO bTable = dbSession.getSession().newInstance(BlockInfoClusterj.BlockInfoDTO.class);
        createPersistable(block, bTable);
        blkChanges.add(bTable);

        //save a new row in the lookup table
        BlockLookUpClusterj.BlockLookUpDTO lookupDTO = dbSession.getSession().newInstance(BlockLookUpClusterj.BlockLookUpDTO.class);
        BlockLookUpClusterj.createPersistable(new HopBlockLookUp(block.getBlockId(), block.getInodeId()), lookupDTO);
        luChanges.add(lookupDTO);
      }
      dbSession.getSession().deletePersistentAll(blkDeletions);
      dbSession.getSession().deletePersistentAll(luDeletions);
      dbSession.getSession().savePersistentAll(blkChanges);
      dbSession.getSession().savePersistentAll(luChanges);
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

      DBSession dbSession = connector.obtainSession();
      BlockInfoClusterj.BlockInfoDTO bit = dbSession.getSession().find(BlockInfoClusterj.BlockInfoDTO.class, pk);
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
      DBSession dbSession = connector.obtainSession();
      QueryBuilder qb = dbSession.getSession().getQueryBuilder();
      QueryDomainType<BlockInfoClusterj.BlockInfoDTO> dobj = qb.createQueryDefinition(BlockInfoClusterj.BlockInfoDTO.class);
      Predicate pred1 = dobj.get("iNodeId").equal(dobj.param("iNodeParam"));
      dobj.where(pred1);
      Query<BlockInfoClusterj.BlockInfoDTO> query = dbSession.getSession().createQuery(dobj);
      query.setParameter("iNodeParam", inodeId);
      return createBlockInfoList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  public HopBlockInfo scanByBlockId(long blockId) throws StorageException {
    try {
      DBSession dbSession = connector.obtainSession();
      QueryBuilder qb = dbSession.getSession().getQueryBuilder();
      QueryDomainType<BlockInfoClusterj.BlockInfoDTO> dobj = qb.createQueryDefinition(BlockInfoClusterj.BlockInfoDTO.class);
      Predicate pred1 = dobj.get("blockId").equal(dobj.param("blockIdParam"));
      dobj.where(pred1);
      Query<BlockInfoClusterj.BlockInfoDTO> query = dbSession.getSession().createQuery(dobj);
      query.setParameter("blockIdParam", blockId);
      return createBlockInfo(query.getResultList().get(0));
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopBlockInfo> findAllBlocks() throws StorageException {
    try {
      DBSession dbSession = connector.obtainSession();
      QueryBuilder qb = dbSession.getSession().getQueryBuilder();
      QueryDomainType<BlockInfoClusterj.BlockInfoDTO> dobj = qb.createQueryDefinition(BlockInfoClusterj.BlockInfoDTO.class);
      Query<BlockInfoClusterj.BlockInfoDTO> query = dbSession.getSession().createQuery(dobj);
      return createBlockInfoList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopBlockInfo> findByStorageId(int storageId) throws StorageException {
    try {
      DBSession dbSession = connector.obtainSession();
      dbSession.getSession().currentTransaction().begin();
      List<ReplicaClusterj.ReplicaDTO> replicas = getReplicas(dbSession, storageId);
      long[] blockIds = new long[replicas.size()];
      for (int i = 0; i < blockIds.length; i++) {
        blockIds[i] = replicas.get(i).getBlockId();
      }

      List<HopBlockInfo> ret = readBlockInfoBatch(dbSession, blockIds);
      dbSession.getSession().currentTransaction().commit();
      return ret;
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopBlockInfo> findByIds(long[] blockIds, int[] inodeIds) throws StorageException {
    try {
      DBSession session = connector.obtainSession();
      return readBlockInfoBatch(session, inodeIds, blockIds);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<Long> findByStorageIdOnlyIds(int storageId) throws StorageException {
    try {
      DBSession session = connector.obtainSession();
      List<ReplicaClusterj.ReplicaDTO> replicas = getReplicas(session, storageId);
      List<Long> blockIds = new ArrayList<Long>();
      for (ReplicaClusterj.ReplicaDTO r : replicas) {
        blockIds.add(r.getBlockId());
      }
      return blockIds;
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private List<ReplicaClusterj.ReplicaDTO> getReplicas(DBSession dbSession, int storageId) {
    QueryBuilder qb = dbSession.getSession().getQueryBuilder();
    QueryDomainType<ReplicaClusterj.ReplicaDTO> dobj = qb.createQueryDefinition(ReplicaClusterj.ReplicaDTO.class);
    dobj.where(dobj.get("storageId").equal(dobj.param("param")));
    Query<ReplicaClusterj.ReplicaDTO> query = dbSession.getSession().createQuery(dobj);
    query.setParameter("param", storageId);
    return query.getResultList();
  }

  private List<HopBlockInfo> readBlockInfoBatch(DBSession session, long[] blockIds) {
    int[] inodeIds = readInodeIds(session, blockIds);
    return readBlockInfoBatch(session, inodeIds, blockIds);
  }

  private int[] readInodeIds(DBSession dbSession, long[] blockIds) {
    List<BlockLookUpClusterj.BlockLookUpDTO> bldtos = new ArrayList<BlockLookUpClusterj.BlockLookUpDTO>();
    int[] inodeIds = new int[blockIds.length];
    for (long blockId : blockIds) {
      BlockLookUpClusterj.BlockLookUpDTO bldto = dbSession.getSession().newInstance(BlockLookUpClusterj.BlockLookUpDTO.class, blockId);
      bldto.setINodeId(NOT_FOUND_ROW);
      bldto = dbSession.getSession().load(bldto);
      bldtos.add(bldto);
    }
    dbSession.getSession().flush();
    for (int i = 0; i < bldtos.size(); i++) {
      BlockLookUpClusterj.BlockLookUpDTO bld = bldtos.get(i);
      inodeIds[i] = bld.getINodeId();
    }
    return inodeIds;
  }
  
  private List<HopBlockInfo> readBlockInfoBatch(DBSession dbSession, int[] inodeIds, long[] blockIds) {
    List<BlockInfoClusterj.BlockInfoDTO> bdtos = new ArrayList<BlockInfoDTO>();
    for (int i = 0; i < blockIds.length; i++) {
      Object[] pk = new Object[]{inodeIds[i], blockIds[i]};
      BlockInfoClusterj.BlockInfoDTO bdto = dbSession.getSession().newInstance(BlockInfoClusterj.BlockInfoDTO.class, pk);
      bdto.setBlockIndex(NOT_FOUND_ROW);
      bdto = dbSession.getSession().load(bdto);
      bdtos.add(bdto);
    }
    dbSession.getSession().flush();
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
