package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.google.common.primitives.Ints;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.dal.CorruptReplicaDataAccess;
import se.sics.hop.metadata.hdfs.entity.hop.HopCorruptReplica;
import se.sics.hop.metadata.hdfs.tabledef.CorruptReplicaTableDef;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.mysqlserver.MySQLQueryHelper;
import se.sics.hop.metadata.ndb.wrapper.HopsPredicate;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CorruptReplicaClusterj implements CorruptReplicaTableDef, CorruptReplicaDataAccess<HopCorruptReplica> {


  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column=INODE_ID)
  @Index(name="timestamp")
  public interface CorruptReplicaDTO {
    @PrimaryKey
    @Column(name = INODE_ID)
    int getINodeId();
    void setINodeId(int inodeId);
    
    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();
    void setBlockId(long bid);

    @PrimaryKey
    @Column(name = STORAGE_ID)
    int getStorageId();
    void setStorageId(int id);
    
    @Column(name = TIMESTAMP)
    long getTimestamp();
    void setTimestamp(long timestamp);
  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public int countAll() throws StorageException {
    return MySQLQueryHelper.countAll(TABLE_NAME);
  }

  @Override
  public int countAllUniqueBlk() throws StorageException {
    return MySQLQueryHelper.countAllUnique(TABLE_NAME, BLOCK_ID);
  }

  @Override
  public void prepare(Collection<HopCorruptReplica> removed, Collection<HopCorruptReplica> newed, Collection<HopCorruptReplica> modified) throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    List<CorruptReplicaDTO> changes = new ArrayList<CorruptReplicaDTO>();
    List<CorruptReplicaDTO> deletions = new ArrayList<CorruptReplicaDTO>();
    for (HopCorruptReplica corruptReplica : removed) {
      CorruptReplicaDTO newInstance = dbSession.newInstance(
          CorruptReplicaDTO.class);
      createPersistable(corruptReplica, newInstance);
      deletions.add(newInstance);
    }

    for (HopCorruptReplica corruptReplica : newed) {
      CorruptReplicaDTO newInstance = dbSession.newInstance(
          CorruptReplicaDTO.class);
      createPersistable(corruptReplica, newInstance);
      changes.add(newInstance);
    }
    dbSession.deletePersistentAll(deletions);
    dbSession.savePersistentAll(changes);
  }

  @Override
  public HopCorruptReplica findByPk(long blockId, int storageId, int inodeId) throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    Object[] keys = new Object[2];
    keys[0] = inodeId;
    keys[1] = blockId;
    keys[2] = storageId;

    CorruptReplicaDTO corruptReplicaTable = dbSession.find(
        CorruptReplicaDTO.class, keys);
    if (corruptReplicaTable != null) {
      return createReplica(corruptReplicaTable);
    } else {
      return null;
    }
  }

  @Override
  public List<HopCorruptReplica> findAll() throws StorageException {
    HopsSession dbSession= connector.obtainSession();
    HopsQueryBuilder qb = dbSession.getQueryBuilder();
    HopsQueryDomainType<CorruptReplicaDTO> dobj = qb.createQueryDefinition(CorruptReplicaDTO.class);
    HopsQuery<CorruptReplicaDTO> query = dbSession.createQuery(dobj);
    query.setOrdering(Query.Ordering.ASCENDING, "timestamp");
    List<CorruptReplicaDTO> ibts = query.getResultList();
    return createCorruptReplicaList(ibts);
  }

  @Override
  public List<HopCorruptReplica> findByBlockId(long blockId, int inodeId) throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    HopsQueryBuilder qb = dbSession.getQueryBuilder();
    HopsQueryDomainType<CorruptReplicaDTO> dobj = qb.createQueryDefinition(CorruptReplicaDTO.class);
    HopsPredicate pred1 = dobj.get("blockId").equal(dobj.param("blockId"));
    HopsPredicate pred2 = dobj.get("iNodeId").equal(dobj.param("iNodeIdParam"));
    dobj.where(pred1.and(pred2));
    HopsQuery<CorruptReplicaDTO> query = dbSession.createQuery(dobj);
    query.setParameter("blockId", blockId);
    query.setParameter("iNodeIdParam", inodeId);
    List<CorruptReplicaDTO> creplicas = query.getResultList();
    return createCorruptReplicaList(creplicas);
  }
  
  @Override
  public List<HopCorruptReplica> findByINodeId(int inodeId) throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    HopsQueryBuilder qb = dbSession.getQueryBuilder();
    HopsQueryDomainType<CorruptReplicaDTO> dobj = qb.createQueryDefinition(CorruptReplicaDTO.class);
    HopsPredicate pred1 = dobj.get("iNodeId").equal(dobj.param("iNodeIdParam"));
    dobj.where(pred1);
    HopsQuery<CorruptReplicaDTO> query = dbSession.createQuery(dobj);
    query.setParameter("iNodeIdParam", inodeId);
    return createCorruptReplicaList(query.getResultList());
  }

  @Override
  public List<HopCorruptReplica> findByINodeIds(int[] inodeIds) throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    HopsQueryBuilder qb = dbSession.getQueryBuilder();
    HopsQueryDomainType<CorruptReplicaDTO> dobj = qb.createQueryDefinition(CorruptReplicaDTO.class);
    HopsPredicate pred1 = dobj.get("iNodeId").in(dobj.param("iNodeIdParam"));
    dobj.where(pred1);
    HopsQuery<CorruptReplicaDTO> query = dbSession.createQuery(dobj);
    query.setParameter("iNodeIdParam", Ints.asList(inodeIds));
    return createCorruptReplicaList(query.getResultList());
  }

  private HopCorruptReplica createReplica(CorruptReplicaDTO corruptReplicaTable) {
    return new HopCorruptReplica(corruptReplicaTable.getBlockId(), corruptReplicaTable.getStorageId(), corruptReplicaTable.getINodeId());
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
    corruptReplicaTable.setTimestamp(System.currentTimeMillis());
  }
}
