package se.sics.hop.metadata.ndb.dalimpl.hdfs;

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
import se.sics.hop.metadata.hdfs.entity.hop.HopCorruptReplica;
import se.sics.hop.metadata.hdfs.dal.CorruptReplicaDataAccess;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.mysqlserver.CountHelper;
import se.sics.hop.metadata.hdfs.tabledef.CorruptReplicaTableDef;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public class CorruptReplicaClusterj implements CorruptReplicaTableDef, CorruptReplicaDataAccess<HopCorruptReplica> {


  @PersistenceCapable(table = TABLE_NAME)
  public interface CorruptReplicaDTO {

    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();
    void setBlockId(long bid);
    
    @PrimaryKey
    @Column(name = INODE_ID)
    int getINodeId();
    void setINodeId(int inodeId);
    
    @PrimaryKey
    @Column(name = PART_KEY)
    int getPartKey();
    void setPartKey(int partKey );

    @PrimaryKey
    @Column(name = STORAGE_ID)
    int getStorageId();
    void setStorageId(int id);
  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public int countAll() throws StorageException {
    return CountHelper.countAll(TABLE_NAME);
  }

  @Override
  public int countAllUniqueBlk() throws StorageException {
    return CountHelper.countAllUnique(TABLE_NAME, BLOCK_ID);
  }

  @Override
  public void prepare(Collection<HopCorruptReplica> removed, Collection<HopCorruptReplica> newed, Collection<HopCorruptReplica> modified) throws StorageException {
    Session session = connector.obtainSession();
    for (HopCorruptReplica corruptReplica : removed) {
      Object[] pk = new Object[4];
      pk[0] = corruptReplica.getBlockId();
      pk[1] = corruptReplica.getStorageId();
      pk[2] = corruptReplica.getInodeId();
      pk[3] = corruptReplica.getPartKey();
      session.deletePersistent(CorruptReplicaDTO.class, pk);
    }

    for (HopCorruptReplica corruptReplica : newed) {
      CorruptReplicaDTO newInstance = session.newInstance(CorruptReplicaDTO.class);
      createPersistable(corruptReplica, newInstance);
      session.savePersistent(newInstance);
    }
  }

  @Override
  public HopCorruptReplica findByPk(long blockId, int storageId, int inodeId, int partKey) throws StorageException {
    try {
      Session session = connector.obtainSession();
      Object[] keys = new Object[2];
      keys[0] = blockId;
      keys[1] = storageId;
      keys[2] = inodeId;
      keys[3] = partKey;
      CorruptReplicaDTO corruptReplicaTable = session.find(CorruptReplicaDTO.class, keys);
      if (corruptReplicaTable != null) {
        return createReplica(corruptReplicaTable);
      } else {
        return null;
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopCorruptReplica> findAll() throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<CorruptReplicaDTO> dobj = qb.createQueryDefinition(CorruptReplicaDTO.class);
      Query<CorruptReplicaDTO> query = session.createQuery(dobj);
      List<CorruptReplicaDTO> ibts = query.getResultList();
      return createCorruptReplicaList(ibts);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopCorruptReplica> findByBlockId(long blockId, int inodeId, int partKey) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<CorruptReplicaDTO> dobj = qb.createQueryDefinition(CorruptReplicaDTO.class);
      Predicate pred1 = dobj.get("blockId").equal(dobj.param("blockId"));
      Predicate pred2 = dobj.get("iNodeId").equal(dobj.param("iNodeIdParam"));
      Predicate pred3 = dobj.get("partKey").equal(dobj.param("partKeyParam"));
      dobj.where(pred1.and(pred2).and(pred3));
      Query<CorruptReplicaDTO> query = session.createQuery(dobj);
      query.setParameter("blockId", blockId);
      query.setParameter("iNodeIdParam", inodeId);
      query.setParameter("partKeyParam", partKey);
      List<CorruptReplicaDTO> creplicas = query.getResultList();
      return createCorruptReplicaList(creplicas);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }
  
    @Override
  public List<HopCorruptReplica> findByINodeId(int inodeId, int partKey) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<CorruptReplicaDTO> dobj = qb.createQueryDefinition(CorruptReplicaDTO.class);
      Predicate pred1 = dobj.get("iNodeId").equal(dobj.param("iNodeIdParam"));
      Predicate pred2 = dobj.get("partKey").equal(dobj.param("partKeyParam"));
      dobj.where(pred1.and(pred2));
      Query<CorruptReplicaDTO> query = session.createQuery(dobj);
      query.setParameter("iNodeIdParam", inodeId);
      query.setParameter("partKeyParam", partKey);
      return createCorruptReplicaList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }


  private HopCorruptReplica createReplica(CorruptReplicaDTO corruptReplicaTable) {
    return new HopCorruptReplica(corruptReplicaTable.getBlockId(), corruptReplicaTable.getStorageId(), corruptReplicaTable.getINodeId(), corruptReplicaTable.getPartKey());
  }

  private List<HopCorruptReplica> createCorruptReplicaList(List<CorruptReplicaDTO> persistables) {
    List<HopCorruptReplica> replicas = new ArrayList<HopCorruptReplica>();
    for (CorruptReplicaDTO bit : persistables) {
      replicas.add(createReplica(bit));
    }
    return replicas;
  }

  private void createPersistable(HopCorruptReplica corruptReplica, CorruptReplicaDTO corruptReplicaTable) {
    corruptReplicaTable.setBlockId(corruptReplica.getBlockId());
    corruptReplicaTable.setStorageId(corruptReplica.getStorageId());
    corruptReplicaTable.setINodeId(corruptReplica.getInodeId());
    corruptReplicaTable.setPartKey(corruptReplica.getPartKey());
  }
}
