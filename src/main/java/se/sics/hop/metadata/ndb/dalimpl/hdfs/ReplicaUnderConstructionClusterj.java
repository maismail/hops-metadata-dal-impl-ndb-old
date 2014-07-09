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
import se.sics.hop.metadata.hdfs.dal.ReplicaUnderConstructionDataAccess;
import se.sics.hop.metadata.hdfs.entity.hdfs.HopReplicaUnderConstruction;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.hdfs.tabledef.ReplicaUnderConstructionTableDef;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ReplicaUnderConstructionClusterj implements ReplicaUnderConstructionTableDef, ReplicaUnderConstructionDataAccess<HopReplicaUnderConstruction> {

  @PersistenceCapable(table = TABLE_NAME)
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
  public void prepare(Collection<HopReplicaUnderConstruction> removed, Collection<HopReplicaUnderConstruction> newed, Collection<HopReplicaUnderConstruction> modified) throws StorageException {
    Session session = connector.obtainSession();
    List<ReplicaUcDTO> changes = new ArrayList<ReplicaUcDTO>();
    List<ReplicaUcDTO> deletions = new ArrayList<ReplicaUcDTO>();
    for (HopReplicaUnderConstruction replica : removed) {
      ReplicaUcDTO newInstance = session.newInstance(ReplicaUcDTO.class);
      createPersistable(replica, newInstance);
      deletions.add(newInstance);
    }

    for (HopReplicaUnderConstruction replica : newed) {
      ReplicaUcDTO newInstance = session.newInstance(ReplicaUcDTO.class);
      createPersistable(replica, newInstance);
      changes.add(newInstance);
    }
    session.deletePersistentAll(deletions);
    session.savePersistentAll(changes);
  }

  @Override
  public List<HopReplicaUnderConstruction> findReplicaUnderConstructionByBlockId(long blockId, int inodeId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<ReplicaUcDTO> dobj = qb.createQueryDefinition(ReplicaUcDTO.class);
      Predicate pred1 = dobj.get("blockId").equal(dobj.param("blockIdParam"));
      Predicate pred2 = dobj.get("iNodeId").equal(dobj.param("iNodeIdParam"));
      dobj.where(pred1.and(pred2));
      Query<ReplicaUcDTO> query = session.createQuery(dobj);
      query.setParameter("blockIdParam", blockId);
      query.setParameter("iNodeIdParam", inodeId);
      return createReplicaList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }
  
  @Override
  public List<HopReplicaUnderConstruction> findReplicaUnderConstructionByINodeId(int inodeId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<ReplicaUcDTO> dobj = qb.createQueryDefinition(ReplicaUcDTO.class);
      Predicate pred1 = dobj.get("iNodeId").equal(dobj.param("iNodeIdParam"));
      dobj.where(pred1);
      Query<ReplicaUcDTO> query = session.createQuery(dobj);
      query.setParameter("iNodeIdParam", inodeId);
      return createReplicaList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }
  

  private List<HopReplicaUnderConstruction> createReplicaList(List<ReplicaUcDTO> replicaUc) {
    Session session = connector.obtainSession();
    List<HopReplicaUnderConstruction> replicas = new ArrayList<HopReplicaUnderConstruction>(replicaUc.size());
    for (ReplicaUcDTO t : replicaUc) {
      replicas.add(new HopReplicaUnderConstruction(t.getState(), t.getStorageId(), t.getBlockId(), t.getINodeId(), t.getIndex()));
    }
    return replicas;
  }

  private void createPersistable(HopReplicaUnderConstruction replica, ReplicaUcDTO newInstance) {
    newInstance.setBlockId(replica.getBlockId());
    newInstance.setIndex(replica.getIndex());
    newInstance.setStorageId(replica.getStorageId());
    newInstance.setState(replica.getState());
    newInstance.setINodeId(replica.getInodeId());
  }
}
