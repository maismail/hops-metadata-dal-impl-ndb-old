package io.hops.metadata.ndb.dalimpl.hdfs;

import com.google.common.primitives.Ints;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.dal.ReplicaUnderConstructionDataAccess;
import io.hops.metadata.hdfs.tabledef.ReplicaUnderConstructionTableDef;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.hdfs.entity.ReplicaUnderConstruction;

public class ReplicaUnderConstructionClusterj implements
    ReplicaUnderConstructionTableDef,
    ReplicaUnderConstructionDataAccess<ReplicaUnderConstruction> {

  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column=INODE_ID)
  public interface ReplicaUcDTO {

    @PrimaryKey
    @Column(name = INODE_ID)
    int getINodeId();
    void setINodeId(int inodeID);
    
    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();
    void setBlockId(long blkid);

    @PrimaryKey
    @Column(name = STORAGE_ID)
    int getStorageId();
    void setStorageId(int id);

    @Column(name = REPLICA_INDEX)
    int getIndex();
    void setIndex(int index);

    @Column(name = STATE)
    int getState();
    void setState(int state);
  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public void prepare(Collection<ReplicaUnderConstruction> removed, Collection<ReplicaUnderConstruction> newed, Collection<ReplicaUnderConstruction> modified) throws
      StorageException {
    HopsSession session = connector.obtainSession();
    List<ReplicaUcDTO> changes = new ArrayList<ReplicaUcDTO>();
    List<ReplicaUcDTO> deletions = new ArrayList<ReplicaUcDTO>();
    for (ReplicaUnderConstruction replica : removed) {
      ReplicaUcDTO newInstance = session.newInstance(ReplicaUcDTO.class);
      createPersistable(replica, newInstance);
      deletions.add(newInstance);
    }

    for (ReplicaUnderConstruction replica : newed) {
      ReplicaUcDTO newInstance = session.newInstance(ReplicaUcDTO.class);
      createPersistable(replica, newInstance);
      changes.add(newInstance);
    }
    session.deletePersistentAll(deletions);
    session.savePersistentAll(changes);
  }

  @Override
  public List<ReplicaUnderConstruction> findReplicaUnderConstructionByBlockId(long blockId, int inodeId) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ReplicaUcDTO> dobj = qb.createQueryDefinition(ReplicaUcDTO.class);
    HopsPredicate pred1 = dobj.get("blockId").equal(dobj.param("blockIdParam"));
    HopsPredicate pred2 = dobj.get("iNodeId").equal(dobj.param("iNodeIdParam"));
    dobj.where(pred1.and(pred2));
    HopsQuery<ReplicaUcDTO> query = session.createQuery(dobj);
    query.setParameter("blockIdParam", blockId);
    query.setParameter("iNodeIdParam", inodeId);
    return createReplicaList(query.getResultList());
  }
  
  @Override
  public List<ReplicaUnderConstruction> findReplicaUnderConstructionByINodeId(int inodeId) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ReplicaUcDTO> dobj = qb.createQueryDefinition(ReplicaUcDTO.class);
    HopsPredicate pred1 = dobj.get("iNodeId").equal(dobj.param("iNodeIdParam"));
    dobj.where(pred1);
    HopsQuery<ReplicaUcDTO> query = session.createQuery(dobj);
    query.setParameter("iNodeIdParam", inodeId);
    return createReplicaList(query.getResultList());
  }
  

  @Override
  public List<ReplicaUnderConstruction> findReplicaUnderConstructionByINodeIds(int[] inodeIds) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ReplicaUcDTO> dobj = qb.createQueryDefinition(ReplicaUcDTO.class);
    HopsPredicate pred1 = dobj.get("iNodeId").in(dobj.param("iNodeIdParam"));
    dobj.where(pred1);
    HopsQuery<ReplicaUcDTO> query = session.createQuery(dobj);
    query.setParameter("iNodeIdParam", Ints.asList(inodeIds));
    return createReplicaList(query.getResultList());
  }

  private List<ReplicaUnderConstruction> createReplicaList(List<ReplicaUcDTO> replicaUc) throws StorageException {
    List<ReplicaUnderConstruction> replicas = new ArrayList<ReplicaUnderConstruction>(replicaUc.size());
    for (ReplicaUcDTO t : replicaUc) {
      replicas.add(new ReplicaUnderConstruction(t.getState(), t.getStorageId(), t.getBlockId(), t.getINodeId(), t.getIndex()));
    }
    return replicas;
  }

  private void createPersistable(ReplicaUnderConstruction replica, ReplicaUcDTO newInstance) {
    newInstance.setBlockId(replica.getBlockId());
    newInstance.setIndex(replica.getIndex());
    newInstance.setStorageId(replica.getStorageId());
    newInstance.setState(replica.getState());
    newInstance.setINodeId(replica.getInodeId());
  }
}
