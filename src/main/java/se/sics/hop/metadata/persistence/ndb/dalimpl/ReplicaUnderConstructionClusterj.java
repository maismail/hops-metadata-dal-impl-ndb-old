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
import se.sics.hop.metadata.persistence.dal.ReplicaUnderConstructionDataAccess;
import se.sics.hop.metadata.persistence.entity.hdfs.HopReplicaUnderConstruction;
import se.sics.hop.metadata.persistence.exceptions.StorageException;
import se.sics.hop.metadata.persistence.ndb.ClusterjConnector;
import se.sics.hop.metadata.persistence.tabledef.ReplicaUnderConstructionTableDef;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ReplicaUnderConstructionClusterj implements ReplicaUnderConstructionTableDef, ReplicaUnderConstructionDataAccess<HopReplicaUnderConstruction> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface ReplicaUcDTO {

    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();

    void setBlockId(long blkid);

    @PrimaryKey
    @Column(name = STORAGE_ID)
    @Index(name = "idx_datanodeStorage")
    String getStorageId();

    void setStorageId(String id);

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
    for (HopReplicaUnderConstruction replica : removed) {
      Object[] pk = new Object[2];
      pk[0] = replica.getBlockId();
      pk[1] = replica.getStorageId();
      session.deletePersistent(ReplicaUcDTO.class, pk);
    }

    for (HopReplicaUnderConstruction replica : newed) {
      ReplicaUcDTO newInstance = session.newInstance(ReplicaUcDTO.class);
      createPersistable(replica, newInstance);
      session.savePersistent(newInstance);
    }
  }

  @Override
  public List<HopReplicaUnderConstruction> findReplicaUnderConstructionByBlockId(long blockId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<ReplicaUcDTO> dobj = qb.createQueryDefinition(ReplicaUcDTO.class);
      dobj.where(dobj.get("blockId").equal(dobj.param("param")));
      Query<ReplicaUcDTO> query = session.createQuery(dobj);
      query.setParameter("param", blockId);
      return createReplicaList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private List<HopReplicaUnderConstruction> createReplicaList(List<ReplicaUcDTO> replicaUc) {
    Session session = connector.obtainSession();
    List<HopReplicaUnderConstruction> replicas = new ArrayList<HopReplicaUnderConstruction>(replicaUc.size());
    for (ReplicaUcDTO t : replicaUc) {
      replicas.add(new HopReplicaUnderConstruction(t.getState(), t.getStorageId(), t.getBlockId(), t.getIndex()));
    }
    return replicas;
  }

  private void createPersistable(HopReplicaUnderConstruction replica, ReplicaUcDTO newInstance) {
    newInstance.setBlockId(replica.getBlockId());
    newInstance.setIndex(replica.getIndex());
    newInstance.setStorageId(replica.getStorageId());
    newInstance.setState(replica.getState());
  }
}
