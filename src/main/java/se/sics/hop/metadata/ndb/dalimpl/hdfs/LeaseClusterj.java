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
import org.apache.log4j.Logger;
import se.sics.hop.metadata.hdfs.dal.LeaseDataAccess;
import se.sics.hop.metadata.hdfs.entity.hdfs.HopLease;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.mysqlserver.CountHelper;
import se.sics.hop.metadata.hdfs.tabledef.LeaseTableDef;

/**
 *
 * @author Hooman <hooman@sics.se>
 * @author salman <salman@sics.se>
 */
public class LeaseClusterj implements LeaseTableDef, LeaseDataAccess<HopLease> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface LeaseDTO {

    @PrimaryKey
    @Column(name = HOLDER)
    String getHolder();
    void setHolder(String holder);
    
    @PrimaryKey
    @Column(name = PART_KEY)
    int getPartKey();
    void setPartKey(int partKey);

    @Column(name = LAST_UPDATE)
    long getLastUpdate();
    void setLastUpdate(long last_upd);

    @Column(name = HOLDER_ID)
    int getHolderId();
    void setHolderId(int holder_id);
  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();
  private static Logger log = Logger.getLogger(LeaseDataAccess.class);

  @Override
  public int countAll() throws StorageException {
    return CountHelper.countAll(TABLE_NAME);
  }

  @Override
  public HopLease findByPKey(String holder) throws StorageException {
    try {
      Session session = connector.obtainSession();
      Object[] key = new Object[2];
      key[0] = holder;
      key[1] = PART_KEY_VAL;
      LeaseDTO lTable = session.find(LeaseDTO.class, key);
      if (lTable != null) {
        HopLease lease = createLease(lTable);
        return lease;
      }
      return null;
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public HopLease findByHolderId(int holderId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<LeaseDTO> dobj = qb.createQueryDefinition(LeaseDTO.class);

      Predicate pred1 = dobj.get("holderId").equal(dobj.param("param1"));
      Predicate pred2 = dobj.get("partKey").equal(dobj.param("param2"));
      
      dobj.where(pred1/*.and(pred2)*/);

      Query<LeaseDTO> query = session.createQuery(dobj);
      query.setParameter("param1", holderId); //the WHERE clause of SQL
      query.setParameter("param2", PART_KEY_VAL);
      List<LeaseDTO> leaseTables = query.getResultList();

      if (leaseTables.size() > 1) {
        log.error("Error in selectLeaseTableInternal: Multiple rows with same holderID");
        return null;
      } else if (leaseTables.size() == 1) {
        HopLease lease = createLease(leaseTables.get(0));
        return lease;
      } else {
        log.info("No rows found for holderID:" + holderId + " in Lease table");
        return null;
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public Collection<HopLease> findAll() throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<LeaseDTO> dobj = qb.createQueryDefinition(LeaseDTO.class);
      Predicate pred = dobj.get("partKey").equal(dobj.param("param"));
      dobj.where(pred);
      Query<LeaseDTO> query = session.createQuery(dobj);
      query.setParameter("param", PART_KEY_VAL);
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public Collection<HopLease> findByTimeLimit(long timeLimit) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType dobj = qb.createQueryDefinition(LeaseDTO.class);
      PredicateOperand propertyPredicate = dobj.get("lastUpdate");
      String param = "timelimit";
      PredicateOperand propertyLimit = dobj.param(param);
      Predicate lessThan = propertyPredicate.lessThan(propertyLimit);
      dobj.where(lessThan.and(dobj.get("partKey").equal(dobj.param("partKeyParam"))));
      Query query = session.createQuery(dobj);
      query.setParameter(param, new Long(timeLimit));
      query.setParameter("partKeyParam", PART_KEY_VAL);
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void prepare(Collection<HopLease> removed, Collection<HopLease> newed, Collection<HopLease> modified) throws StorageException {
    try {
      Session session = connector.obtainSession();
      List<LeaseDTO> changes = new ArrayList<LeaseDTO>();
      List<LeaseDTO> deletions = new ArrayList<LeaseDTO>();
      for (HopLease l : newed) {
        LeaseDTO lTable = session.newInstance(LeaseDTO.class);
        createPersistableLeaseInstance(l, lTable);
        changes.add(lTable);
      }

      for (HopLease l : modified) {
        LeaseDTO lTable = session.newInstance(LeaseDTO.class);
        createPersistableLeaseInstance(l, lTable);
        changes.add(lTable);
      }

      for (HopLease l : removed) {
        Object[] key = new Object[2];
        key[0] = l.getHolder();
        key[1] = PART_KEY_VAL;
        LeaseDTO lTable = session.newInstance(LeaseDTO.class, key);
        deletions.add(lTable);
      }
      session.deletePersistentAll(deletions);
      session.savePersistentAll(changes);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private Collection<HopLease> createList(List<LeaseDTO> list) {
    Collection<HopLease> finalSet = new ArrayList<HopLease>();
    for (LeaseDTO dto : list) {
      finalSet.add(createLease(dto));
    }

    return finalSet;
  }

  @Override
  public void removeAll() throws StorageException {
    try {
      Session session = connector.obtainSession();
      session.deletePersistentAll(LeaseDTO.class);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private HopLease createLease(LeaseDTO lTable) {
    return new HopLease(lTable.getHolder(), lTable.getHolderId(), lTable.getLastUpdate());
  }

  private void createPersistableLeaseInstance(HopLease lease, LeaseDTO lTable) {
    lTable.setHolder(lease.getHolder());
    lTable.setHolderId(lease.getHolderId());
    lTable.setLastUpdate(lease.getLastUpdate());
    lTable.setPartKey(PART_KEY_VAL);
  }
}
