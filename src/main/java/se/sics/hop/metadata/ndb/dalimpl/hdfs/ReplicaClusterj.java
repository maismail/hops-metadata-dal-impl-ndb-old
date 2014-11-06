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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.sics.hop.metadata.hdfs.entity.hop.HopIndexedReplica;
import se.sics.hop.metadata.hdfs.dal.ReplicaDataAccess;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.hdfs.tabledef.ReplicaTableDef;
import se.sics.hop.metadata.ndb.mysqlserver.MySQLQueryHelper;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ReplicaClusterj implements ReplicaTableDef, ReplicaDataAccess<HopIndexedReplica> {
  static final Log LOG = LogFactory.getLog(ReplicaClusterj.class);

  @PersistenceCapable(table = TABLE_NAME)
  public interface ReplicaDTO {

    @PrimaryKey
    @Column(name = INODE_ID)
    int getINodeId();
    void setINodeId(int inodeID);
    
    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();
    void setBlockId(long bid);

    @PrimaryKey
    @Column(name = STORAGE_ID)
    @Index(name = "storage_idx")
    int getStorageId();
    void setStorageId(int id);

    @Column(name = REPLICA_INDEX)
    int getIndex();
    void setIndex(int index);
  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();
  private final static int NOT_FOUND_ROW = -1000;
  
  @Override
  public List<HopIndexedReplica> findReplicasById(long blockId, int inodeId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<ReplicaDTO> dobj = qb.createQueryDefinition(ReplicaDTO.class);
      Predicate pred1 = dobj.get("blockId").equal(dobj.param("blockIdParam"));
      Predicate pred2 = dobj.get("iNodeId").equal(dobj.param("iNodeIdParam"));
      dobj.where(pred1.and(pred2));
      Query<ReplicaDTO> query = session.createQuery(dobj);
      query.setParameter("blockIdParam", blockId);
      query.setParameter("iNodeIdParam", inodeId);
      return createReplicaList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }
  
  
  @Override
  public List<HopIndexedReplica> findReplicasByINodeId(int inodeId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<ReplicaDTO> dobj = qb.createQueryDefinition(ReplicaDTO.class);
      Predicate pred1 = dobj.get("iNodeId").equal(dobj.param("iNodeIdParam"));
      dobj.where(pred1);
      Query<ReplicaDTO> query = session.createQuery(dobj);
      query.setParameter("iNodeIdParam", inodeId);
      return createReplicaList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }
  

  @Override
  public List<HopIndexedReplica> findReplicasByINodeIds(int[] inodeIds) throws StorageException {
     try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<ReplicaDTO> dobj = qb.createQueryDefinition(ReplicaDTO.class);
      Predicate pred1 = dobj.get("iNodeId").in(dobj.param("iNodeIdParam"));
      dobj.where(pred1);
      Query<ReplicaDTO> query = session.createQuery(dobj);
      query.setParameter("iNodeIdParam", Ints.asList(inodeIds));
      return createReplicaList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }
  
  @Override
  public List<HopIndexedReplica> findReplicasByStorageId(int storageId) throws StorageException {
   try {
      Session session = connector.obtainSession();
      long t = System.currentTimeMillis();
      List<ReplicaDTO> res = getReplicas(session, storageId);
      //ClusterjConnector.LOG.error("xxxa: got replicas " + res.size() + " in " + (System.currentTimeMillis() - t));
      return createReplicaList(res);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }
    
  @Override
  public List<HopIndexedReplica> findReplicasByPKS(final long[] blockIds, final int[] inodeIds, final int[] sids) throws StorageException {
    try {
      final List<ReplicaDTO> dtos = new ArrayList<ReplicaDTO>();
      Session session = connector.obtainSession();
      for (int i = 0; i < blockIds.length; i++) {
        ReplicaDTO newInstance = session.newInstance(ReplicaDTO.class, new Object[]{inodeIds[i], blockIds[i], sids[i]});
        newInstance.setIndex(NOT_FOUND_ROW);
        newInstance = session.load(newInstance);
        dtos.add(newInstance);
      }
      session.flush();

      return createReplicaList(dtos);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  
  @Override
  public void prepare(Collection<HopIndexedReplica> removed, Collection<HopIndexedReplica> newed, Collection<HopIndexedReplica> modified) throws StorageException {
    try {
      List<ReplicaDTO> changes = new ArrayList<ReplicaDTO>();
      List<ReplicaDTO> deletions = new ArrayList<ReplicaDTO>();
      Session session = connector.obtainSession();
      for (HopIndexedReplica replica : removed) {
        ReplicaDTO newInstance = session.newInstance(ReplicaDTO.class);
        createPersistable(replica, newInstance);
        deletions.add(newInstance);
      }

      for (HopIndexedReplica replica : newed) {
        ReplicaDTO newInstance = session.newInstance(ReplicaDTO.class);
        createPersistable(replica, newInstance);
        changes.add(newInstance);
      }

      for (HopIndexedReplica replica : modified) {
        ReplicaDTO newInstance = session.newInstance(ReplicaDTO.class);
        createPersistable(replica, newInstance);
        changes.add(newInstance);
      }
      session.deletePersistentAll(deletions);
      session.savePersistentAll(changes);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public int countAllReplicasForStorageId(int sid) throws StorageException {
    return MySQLQueryHelper.countWithCriterion(TABLE_NAME, String.format("%s=%d", STORAGE_ID, sid));
  }

  
  protected static List<ReplicaClusterj.ReplicaDTO> getReplicas(Session session, int storageId) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<ReplicaClusterj.ReplicaDTO> dobj = qb.createQueryDefinition(ReplicaClusterj.ReplicaDTO.class);
    dobj.where(dobj.get("storageId").equal(dobj.param("param")));
    Query<ReplicaClusterj.ReplicaDTO> query = session.createQuery(dobj);
    query.setParameter("param", storageId);
    return query.getResultList();
  }

    
  private List<HopIndexedReplica> createReplicaList(List<ReplicaDTO> triplets) {
    List<HopIndexedReplica> replicas = new ArrayList<HopIndexedReplica>(triplets.size());
    for (ReplicaDTO t : triplets) {
      if (t.getIndex() != NOT_FOUND_ROW) {
        replicas.add(new HopIndexedReplica(t.getBlockId(), t.getStorageId(), t.getINodeId(), t.getIndex()));
      }
    }
    return replicas;
  }

  private void createPersistable(HopIndexedReplica replica, ReplicaDTO newInstance) {
    newInstance.setBlockId(replica.getBlockId());
    newInstance.setIndex(replica.getIndex());
    newInstance.setStorageId(replica.getStorageId());
    newInstance.setINodeId(replica.getInodeId());
  }
}