package se.sics.hop.metadata.persistence.ndb.dalimpl;

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
import java.util.List;
import se.sics.hop.metadata.persistence.entity.hop.HopUnderReplicatedBlock;
import se.sics.hop.metadata.persistence.dal.UnderReplicatedBlockDataAccess;
import se.sics.hop.metadata.persistence.exceptions.StorageException;
import se.sics.hop.metadata.persistence.ndb.ClusterjConnector;
import se.sics.hop.metadata.persistence.ndb.mysqlserver.CountHelper;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public class UnderReplicatedBlockClusterj extends UnderReplicatedBlockDataAccess {

  @Override
  public int countByLevel(int level) throws StorageException {
    return CountHelper.countWithCriterion(
            TABLE_NAME,
            String.format("%s=%d", LEVEL, level));
  }

  @Override
  public int countLessThanALevel(int level) throws StorageException {
    return CountHelper.countWithCriterion(
            TABLE_NAME,
            String.format("%s<%d", LEVEL, level));
  }

  @PersistenceCapable(table = TABLE_NAME)
  public interface UnderReplicatedBlocksDTO {

    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();

    void setBlockId(long bid);

    @Column(name = LEVEL)
    int getLevel();

    void setLevel(int level);
  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public HopUnderReplicatedBlock findByBlockId(long blockId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      UnderReplicatedBlocksDTO urbt = session.find(UnderReplicatedBlocksDTO.class, blockId);
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
    Session session = connector.obtainSession();
    for (HopUnderReplicatedBlock urBlock : removed) {
      session.deletePersistent(UnderReplicatedBlocksDTO.class, urBlock.getBlockId());
    }

    for (HopUnderReplicatedBlock urBlock : newed) {
      UnderReplicatedBlocksDTO newInstance = session.newInstance(UnderReplicatedBlocksDTO.class);
      createPersistable(urBlock, newInstance);
      session.savePersistent(newInstance);
    }

    for (HopUnderReplicatedBlock urBlock : modified) {
      UnderReplicatedBlocksDTO newInstance = session.newInstance(UnderReplicatedBlocksDTO.class);
      createPersistable(urBlock, newInstance);
      session.savePersistent(newInstance);
    }
  }

  private void createPersistable(HopUnderReplicatedBlock block, UnderReplicatedBlocksDTO persistable) {
    persistable.setBlockId(block.getBlockId());
    persistable.setLevel(block.getLevel());
  }

  private HopUnderReplicatedBlock createUrBlock(UnderReplicatedBlocksDTO bit) {
    HopUnderReplicatedBlock block = new HopUnderReplicatedBlock(bit.getLevel(), bit.getBlockId());
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
    return CountHelper.countAll(TABLE_NAME);
  }

  @Override
  public List<HopUnderReplicatedBlock> findAll() throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<UnderReplicatedBlocksDTO> dobj = qb.createQueryDefinition(UnderReplicatedBlocksDTO.class);
      Query<UnderReplicatedBlocksDTO> query = session.createQuery(dobj);
      List<UnderReplicatedBlocksDTO> urbks = query.getResultList();
      List<HopUnderReplicatedBlock> blocks = createUrBlockList(urbks);
      return blocks;
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopUnderReplicatedBlock> findByLevel(int level) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<UnderReplicatedBlocksDTO> dobj = qb.createQueryDefinition(UnderReplicatedBlocksDTO.class);
      Predicate pred = dobj.get("level").equal(dobj.param("level"));
      dobj.where(pred);
      Query<UnderReplicatedBlocksDTO> query = session.createQuery(dobj);
      query.setParameter("level", level);
      return createUrBlockList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopUnderReplicatedBlock> findAllLessThanLevel(int level) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<UnderReplicatedBlocksDTO> dobj = qb.createQueryDefinition(UnderReplicatedBlocksDTO.class);
      Predicate pred = dobj.get("level").lessThan(dobj.param("level"));
      dobj.where(pred);
      Query<UnderReplicatedBlocksDTO> query = session.createQuery(dobj);
      query.setParameter("level", level);

      return createUrBlockList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void removeAll() throws StorageException {
    Session session = connector.obtainSession();
    session.deletePersistentAll(UnderReplicatedBlocksDTO.class);
  }
}
