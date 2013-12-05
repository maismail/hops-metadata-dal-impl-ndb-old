package se.sics.hop.metadata.persistence.ndb.dalimpl;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.metadata.persistence.dal.BlockInfoDataAccess;
import static se.sics.hop.metadata.persistence.dal.BlockInfoDataAccess.BLOCK_ID;
import static se.sics.hop.metadata.persistence.dal.BlockInfoDataAccess.BLOCK_INDEX;
import static se.sics.hop.metadata.persistence.dal.BlockInfoDataAccess.BLOCK_RECOVERY_ID;
import static se.sics.hop.metadata.persistence.dal.BlockInfoDataAccess.BLOCK_UNDER_CONSTRUCTION_STATE;
import static se.sics.hop.metadata.persistence.dal.BlockInfoDataAccess.GENERATION_STAMP;
import static se.sics.hop.metadata.persistence.dal.BlockInfoDataAccess.INODE_ID;
import static se.sics.hop.metadata.persistence.dal.BlockInfoDataAccess.NUM_BYTES;
import static se.sics.hop.metadata.persistence.dal.BlockInfoDataAccess.PRIMARY_NODE_INDEX;
import static se.sics.hop.metadata.persistence.dal.BlockInfoDataAccess.TABLE_NAME;
import static se.sics.hop.metadata.persistence.dal.BlockInfoDataAccess.TIME_STAMP;
import se.sics.hop.metadata.persistence.entity.hdfs.HopBlockInfo;
import se.sics.hop.metadata.persistence.exceptions.StorageException;
import se.sics.hop.metadata.persistence.ndb.ClusterjConnector;
import se.sics.hop.metadata.persistence.ndb.mysqlserver.CountHelper;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class BlockInfoClusterj extends BlockInfoDataAccess {

    @PersistenceCapable(table = TABLE_NAME)
    public interface BlockInfoDTO {

        @PrimaryKey
        @Column(name = BLOCK_ID)
        long getBlockId();

        void setBlockId(long bid);

        @Column(name = BLOCK_INDEX)
        int getBlockIndex();

        void setBlockIndex(int idx);

        @Column(name = INODE_ID)
        @Index(name = "idx_inodeid")
        long getINodeId();

        void setINodeId(long iNodeID);

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

    @Override
    public int countAll() throws StorageException {
        return CountHelper.countAll(TABLE_NAME);
    }

    @Override
    public void prepare(Collection<HopBlockInfo> removed, Collection<HopBlockInfo> news, Collection<HopBlockInfo> modified) throws StorageException {
        try {
            Session session = connector.obtainSession();
            for (HopBlockInfo block : removed) {
                BlockInfoClusterj.BlockInfoDTO bTable = session.newInstance(BlockInfoClusterj.BlockInfoDTO.class, block.getBlockId());
                session.deletePersistent(bTable);
            }

            for (HopBlockInfo block : news) {
                BlockInfoClusterj.BlockInfoDTO bTable = session.newInstance(BlockInfoClusterj.BlockInfoDTO.class);
                createPersistable(block, bTable);
                session.savePersistent(bTable);
            }

            for (HopBlockInfo block : modified) {
                BlockInfoClusterj.BlockInfoDTO bTable = session.newInstance(BlockInfoClusterj.BlockInfoDTO.class);
                createPersistable(block, bTable);
                session.savePersistent(bTable);
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public HopBlockInfo findById(long blockId) throws StorageException {
        try {
            Session session = connector.obtainSession();
            BlockInfoClusterj.BlockInfoDTO bit = session.find(BlockInfoClusterj.BlockInfoDTO.class, blockId);
            if (bit == null) {
                return null;
            }
            return createBlockInfo(bit);
        } catch (Exception e) {
            //[S] clusterj new lib 7.3.2 sometimes throws tuple not found exception
            //if the row does not exist. Silly
            if (e.getMessage().contains("Tuple did not exist")) {
                return null;
            } else {
                throw new StorageException(e);
            }
        }
    }

    @Override
    public List<HopBlockInfo> findByInodeId(long id) throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();
            QueryDomainType<BlockInfoClusterj.BlockInfoDTO> dobj = qb.createQueryDefinition(BlockInfoClusterj.BlockInfoDTO.class);
            dobj.where(dobj.get("iNodeId").equal(dobj.param("param")));
            Query<BlockInfoClusterj.BlockInfoDTO> query = session.createQuery(dobj);
            query.setParameter("param", id);
            return createBlockInfoList(query.getResultList());
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

    // TODO - set the Partition KEY before calling this operation
    @Override
    public List<HopBlockInfo> findByStorageId(String storageId) throws StorageException {
        try {
            Session session = connector.obtainSession();
            List<HopBlockInfo> ret = new ArrayList<HopBlockInfo>();
            QueryBuilder qb = session.getQueryBuilder();
            QueryDomainType<ReplicaClusterj.ReplicaDTO> dobj = qb.createQueryDefinition(ReplicaClusterj.ReplicaDTO.class);
            dobj.where(dobj.get("storageId").equal(dobj.param("param")));
            Query<ReplicaClusterj.ReplicaDTO> query = session.createQuery(dobj);
            query.setParameter("param", storageId);
            List<ReplicaClusterj.ReplicaDTO> replicas = query.getResultList();

            for (ReplicaClusterj.ReplicaDTO t : replicas) {
                ret.add(findById(t.getBlockId()));
            }
            return ret;
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    private List<HopBlockInfo> createBlockInfoList(List<BlockInfoClusterj.BlockInfoDTO> bitList) {
        List<HopBlockInfo> list = new ArrayList<HopBlockInfo>();

        for (BlockInfoClusterj.BlockInfoDTO blockInfoDTO : bitList) {
            list.add(createBlockInfo(blockInfoDTO));
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
