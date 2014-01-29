package se.sics.hop.metadata.ndb.dalimpl.hdfs;

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
import se.sics.hop.metadata.hdfs.entity.hop.HopExcessReplica;
import se.sics.hop.metadata.hdfs.dal.ExcessReplicaDataAccess;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.mysqlserver.CountHelper;
import se.sics.hop.metadata.hdfs.tabledef.ExcessReplicaTableDef;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ExcessReplicaClusterj implements ExcessReplicaTableDef, ExcessReplicaDataAccess<HopExcessReplica> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface ExcessReplicaDTO {

    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();

    void setBlockId(long storageId);

    @PrimaryKey
    @Column(name = STORAGE_ID)
    String getStorageId();

    void setStorageId(String storageId);
  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public int countAll() throws StorageException {
    return CountHelper.countAll(TABLE_NAME);
  }

  @Override
  public void prepare(Collection<HopExcessReplica> removed, Collection<HopExcessReplica> newed, Collection<HopExcessReplica> modified) throws StorageException {
    try {
      Session session = connector.obtainSession();
      for (HopExcessReplica exReplica : newed) {
        ExcessReplicaDTO newInstance = session.newInstance(ExcessReplicaDTO.class);
        createPersistable(exReplica, newInstance);
        session.savePersistent(newInstance);
      }

      for (HopExcessReplica exReplica : removed) {
        Object[] pk = new Object[2];
        pk[0] = exReplica.getBlockId();
        pk[1] = exReplica.getStorageId();
        session.deletePersistent(ExcessReplicaDTO.class, pk);
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopExcessReplica> findExcessReplicaByStorageId(String storageId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<ExcessReplicaDTO> qdt = qb.createQueryDefinition(ExcessReplicaDTO.class);
      qdt.where(qdt.get("storageId").equal(qdt.param("param")));
      Query<ExcessReplicaDTO> query = session.createQuery(qdt);
      query.setParameter("param", storageId);
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopExcessReplica> findExcessReplicaByBlockId(long bId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<ExcessReplicaDTO> qdt = qb.createQueryDefinition(ExcessReplicaDTO.class);
      qdt.where(qdt.get("blockId").equal(qdt.param("param")));
      Query<ExcessReplicaDTO> query = session.createQuery(qdt);
      query.setParameter("param", bId);
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public HopExcessReplica findByPkey(Object[] params) throws StorageException {
    try {
      Session session = connector.obtainSession();
      ExcessReplicaDTO invTable = session.find(ExcessReplicaDTO.class, params);
      if (invTable == null) {
        return null;
      }
      HopExcessReplica result = createReplica(invTable);
      return result;
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void removeAll() throws StorageException {
    try {
      Session session = connector.obtainSession();
      session.deletePersistentAll(ExcessReplicaDTO.class);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private List<HopExcessReplica> createList(List<ExcessReplicaDTO> list) {
    List<HopExcessReplica> result = new ArrayList<HopExcessReplica>();
    for (ExcessReplicaDTO item : list) {
      result.add(createReplica(item));
    }
    return result;
  }

  private HopExcessReplica createReplica(ExcessReplicaDTO exReplicaTable) {
    return new HopExcessReplica(exReplicaTable.getStorageId(), exReplicaTable.getBlockId());
  }

  private void createPersistable(HopExcessReplica exReplica, ExcessReplicaDTO exReplicaTable) {
    exReplicaTable.setBlockId(exReplica.getBlockId());
    exReplicaTable.setStorageId(exReplica.getStorageId());
  }
}
