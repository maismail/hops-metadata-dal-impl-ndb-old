package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.PredicateOperand;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.metadata.hdfs.entity.hop.HopLeasePath;
import se.sics.hop.metadata.hdfs.dal.LeasePathDataAccess;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.hdfs.tabledef.LeasePathTableDef;

/**
 *
 * @author Hooman <hooman@sics.se>
 * @author Salman <salman@sics.se>
 */
public class LeasePathClusterj implements LeasePathTableDef, LeasePathDataAccess<HopLeasePath> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface LeasePathsDTO {

    @Column(name = HOLDER_ID)
    int getHolderId();
    void setHolderId(int holder_id);

    @PrimaryKey
    @Column(name = PATH)
    String getPath();
    void setPath(String path);
    
    @PrimaryKey
    @Column(name = PART_KEY)
    int getPartKey();
    void setPartKey(int partKey);
  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public void prepare(Collection<HopLeasePath> removed, Collection<HopLeasePath> newed, Collection<HopLeasePath> modified) throws StorageException {
    try {
      List<LeasePathsDTO> changes = new ArrayList<LeasePathsDTO>();
      List<LeasePathsDTO> deletions = new ArrayList<LeasePathsDTO>();
      Session session = connector.obtainSession();
      for (HopLeasePath lp : newed) {
        LeasePathsDTO lTable = session.newInstance(LeasePathsDTO.class);
        createPersistableLeasePathInstance(lp, lTable);
        changes.add(lTable);
      }

      for (HopLeasePath lp : modified) {
        LeasePathsDTO lTable = session.newInstance(LeasePathsDTO.class);
        createPersistableLeasePathInstance(lp, lTable);
        changes.add(lTable);
      }

      for (HopLeasePath lp : removed) {
        Object[] key = new Object[2];
        key[0] = lp.getPath();
        key[1] = PART_KEY_VAL;
        LeasePathsDTO lTable = session.newInstance(LeasePathsDTO.class, key);
        deletions.add(lTable);
      }
      session.deletePersistentAll(deletions);
      session.savePersistentAll(changes);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public Collection<HopLeasePath> findByHolderId(int holderId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<LeasePathsDTO> dobj = qb.createQueryDefinition(LeasePathsDTO.class);
      Predicate pred1 = dobj.get("holderId").equal(dobj.param("param1"));
      Predicate pred2 = dobj.get("partKey").equal(dobj.param("param2"));
      dobj.where(pred1);
      Query<LeasePathsDTO> query = session.createQuery(dobj);
      query.setParameter("param1", holderId);
      query.setParameter("param2", PART_KEY_VAL);
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public HopLeasePath findByPKey(String path) throws StorageException {
    try {
      Object[] key = new Object[2];
      key[0] = path;
      key[1] = PART_KEY_VAL;
      Session session = connector.obtainSession();
      LeasePathsDTO lPTable = session.find(LeasePathsDTO.class, key);
      HopLeasePath lPath = null;
      if (lPTable != null) {
        lPath = createLeasePath(lPTable);
      }
      return lPath;
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public Collection<HopLeasePath> findByPrefix(String prefix) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType dobj = qb.createQueryDefinition(LeasePathsDTO.class);
      PredicateOperand propertyPredicate = dobj.get("path");
      String param = "prefix";
      PredicateOperand propertyLimit = dobj.param(param);
      Predicate like = propertyPredicate.like(propertyLimit).and(dobj.get("partKey").equal(dobj.param("partKeyParam")));
      dobj.where(like);
      Query query = session.createQuery(dobj);
      query.setParameter(param, prefix + "%");
      query.setParameter("partKeyParam", PART_KEY_VAL);
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public Collection<HopLeasePath> findAll() throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType dobj = qb.createQueryDefinition(LeasePathsDTO.class);
      Predicate pred = dobj.get("partKey").equal(dobj.param("param"));
      dobj.where(pred);
      Query query = session.createQuery(dobj);
      query.setParameter("param", PART_KEY_VAL);
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void removeAll() throws StorageException {
    try {
      Session session = connector.obtainSession();
      session.deletePersistentAll(LeasePathsDTO.class);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private List<HopLeasePath> createList(Collection<LeasePathsDTO> dtos) {
    List<HopLeasePath> list = new ArrayList<HopLeasePath>();
    for (LeasePathsDTO leasePathsDTO : dtos) {
      list.add(createLeasePath(leasePathsDTO));
    }
    return list;
  }

  private HopLeasePath createLeasePath(LeasePathsDTO leasePathTable) {
    return new HopLeasePath(leasePathTable.getPath(), leasePathTable.getHolderId());
  }

  private void createPersistableLeasePathInstance(HopLeasePath lp, LeasePathsDTO lTable) {
    lTable.setHolderId(lp.getHolderId());
    lTable.setPath(lp.getPath());
    lTable.setPartKey(PART_KEY_VAL);
  }
}
