package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.google.common.primitives.Ints;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.dal.BlockLookUpDataAccess;
import se.sics.hop.metadata.hdfs.entity.hop.HopBlockLookUp;
import se.sics.hop.metadata.hdfs.tabledef.BlockLookUpTableDef;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.util.Slicer;

/**
 *
 * @author salman <salman@sics.se>
 */
public class BlockLookUpClusterj implements BlockLookUpTableDef, BlockLookUpDataAccess<HopBlockLookUp> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface BlockLookUpDTO {

    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();

    void setBlockId(long bid);

    @Column(name = INODE_ID)
    int getINodeId();

    void setINodeId(int iNodeID);

  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();
  private final static int NOT_FOUND_ROW = -1000;

  @Override
  public void prepare(Collection<HopBlockLookUp> modified, Collection<HopBlockLookUp> removed) throws StorageException {
    try {
      Session session = connector.obtainSession();
      for (HopBlockLookUp block_lookup : removed) {
        BlockLookUpClusterj.BlockLookUpDTO bTable = session.newInstance(BlockLookUpClusterj.BlockLookUpDTO.class, block_lookup.getBlockId());
        session.deletePersistent(bTable);
      }

      for (HopBlockLookUp block_lookup : modified) {
        BlockLookUpClusterj.BlockLookUpDTO bTable = session.newInstance(BlockLookUpClusterj.BlockLookUpDTO.class);
        createPersistable(block_lookup, bTable);
        session.savePersistent(bTable);
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public HopBlockLookUp findByBlockId(long blockId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      BlockLookUpClusterj.BlockLookUpDTO lookup = session.find(BlockLookUpClusterj.BlockLookUpDTO.class, blockId);
      if (lookup == null) {
        return null;
      }
      return createBlockInfo(lookup);
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
  public int[] findINodeIdsByBlockIds(final long[] blockIds) throws StorageException {
    try {
      final Session session = connector.obtainSession();
      return readINodeIdsByBlockIds(session, blockIds);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  protected static int[] readINodeIdsByBlockIds(final Session session, final long[] blockIds) throws Exception {
    final List<BlockLookUpDTO> bldtos = new ArrayList<BlockLookUpDTO>();
    final List<Integer> inodeIds = new ArrayList<Integer>();

    Slicer.slice(blockIds.length, ClusterjConnector.getInstance().getBatchSize(), new Slicer.OperationHandler() {
      @Override
      public void handle(int startIndex, int endIndex) throws Exception {

        for (int blk = startIndex; blk < endIndex; blk++) {

          BlockLookUpDTO bldto = session.newInstance(BlockLookUpDTO.class, blockIds[blk]);
          bldto.setINodeId(NOT_FOUND_ROW);
          bldto = session.load(bldto);
          bldtos.add(bldto);
        }
        session.flush();

        session.currentTransaction().commit();
        session.currentTransaction().begin();

        for (int i = 0; i < bldtos.size(); i++) {
          BlockLookUpClusterj.BlockLookUpDTO bld = bldtos.get(i);
          if (bld.getINodeId() != NOT_FOUND_ROW) {
            inodeIds.add(bld.getINodeId());
          } else {
            bld = session.find(BlockLookUpDTO.class, bld.getBlockId());
            if (bld != null) {
              //[M] BUG:
              inodeIds.add(bld.getINodeId());
            }

          }
        }
        bldtos.clear();
      }
    });
    return Ints.toArray(inodeIds);
  }
  
  protected static HopBlockLookUp createBlockInfo(BlockLookUpClusterj.BlockLookUpDTO dto) {
    HopBlockLookUp lookup = new HopBlockLookUp(dto.getBlockId(), dto.getINodeId());
    return lookup;
  }

  protected static void createPersistable(HopBlockLookUp lookup, BlockLookUpClusterj.BlockLookUpDTO persistable) {
    persistable.setBlockId(lookup.getBlockId());
    persistable.setINodeId(lookup.getInodeId());
  }
}
