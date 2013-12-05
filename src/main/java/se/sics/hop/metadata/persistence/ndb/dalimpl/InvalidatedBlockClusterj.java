package se.sics.hop.metadata.persistence.ndb.dalimpl;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.metadata.persistence.entity.hop.HopInvalidatedBlock;
import se.sics.hop.metadata.persistence.dal.InvalidateBlockDataAccess;
import se.sics.hop.metadata.persistence.exceptions.StorageException;
import se.sics.hop.metadata.persistence.ndb.ClusterjConnector;
import se.sics.hop.metadata.persistence.ndb.mysqlserver.CountHelper;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class InvalidatedBlockClusterj extends InvalidateBlockDataAccess {


  @PersistenceCapable(table = TABLE_NAME)
  public interface InvalidateBlocksDTO {

    @PrimaryKey
    @Column(name = STORAGE_ID)
    String getStorageId();

    void setStorageId(String storageId);

    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();

    void setBlockId(long storageId);

    @Column(name = GENERATION_STAMP)
    long getGenerationStamp();

    void setGenerationStamp(long generationStamp);

    @Column(name = NUM_BYTES)
    long getNumBytes();

    void setNumBytes(long numBytes);
  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();

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
  public List<HopInvalidatedBlock> findInvalidatedBlockByStorageId(String storageId) throws StorageException {
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
  public Collection<HopInvalidatedBlock> findInvalidatedBlocksByBlockId(long bid) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<InvalidateBlocksDTO> qdt = qb.createQueryDefinition(InvalidateBlocksDTO.class);
      qdt.where(qdt.get("blockId").equal(qdt.param("param")));
      Query<InvalidateBlocksDTO> query = session.createQuery(qdt);
      query.setParameter("param", bid);
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public HopInvalidatedBlock findInvBlockByPkey(Object[] params) throws StorageException {
    try {
      Session session = connector.obtainSession();
      InvalidateBlocksDTO invTable = session.find(InvalidateBlocksDTO.class, params);
      if (invTable == null) {
        return null;
      }
      return createReplica(invTable);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void prepare(Collection<HopInvalidatedBlock> removed, Collection<HopInvalidatedBlock> newed, Collection<HopInvalidatedBlock> modified) throws StorageException {
    try {
      Session session = connector.obtainSession();
      for (HopInvalidatedBlock invBlock : newed) {
        InvalidateBlocksDTO newInstance = session.newInstance(InvalidateBlocksDTO.class);
        createPersistable(invBlock, newInstance);
        session.savePersistent(newInstance);
      }

      for (HopInvalidatedBlock invBlock : removed) {
        remove(invBlock);
      }
      
      if(!modified.isEmpty())
      {
        throw new UnsupportedOperationException("Not yet Implemented");
      }
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
  public void remove(HopInvalidatedBlock invBlock) {
    Session session = connector.obtainSession();
    Object[] pk = new Object[2];
    pk[0] = invBlock.getBlockId();
    pk[1] = invBlock.getStorageId();
    session.deletePersistent(InvalidateBlocksDTO.class, pk);
  }
   
  private List<HopInvalidatedBlock> createList(List<InvalidateBlocksDTO> dtoList) {
    List<HopInvalidatedBlock> list = new ArrayList<HopInvalidatedBlock>();
    for (InvalidateBlocksDTO dto : dtoList) {
      list.add(createReplica(dto));
    }
    return list;
  }

  private HopInvalidatedBlock createReplica(InvalidateBlocksDTO invBlockTable) {
    return new HopInvalidatedBlock(invBlockTable.getStorageId(), invBlockTable.getBlockId(),
            invBlockTable.getGenerationStamp(), invBlockTable.getNumBytes());
  }

  private void createPersistable(HopInvalidatedBlock invBlock, InvalidateBlocksDTO newInvTable) {
    newInvTable.setBlockId(invBlock.getBlockId());
    newInvTable.setStorageId(invBlock.getStorageId());
    newInvTable.setGenerationStamp(invBlock.getGenerationStamp());
    newInvTable.setNumBytes(invBlock.getNumBytes());
  }
}
