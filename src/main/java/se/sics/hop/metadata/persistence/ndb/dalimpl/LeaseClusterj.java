package se.sics.hop.metadata.persistence.ndb.dalimpl;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.PredicateOperand;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import se.sics.hop.metadata.persistence.dal.LeaseDataAccess;
import se.sics.hop.metadata.persistence.entity.hdfs.HopLease;
import se.sics.hop.metadata.persistence.exceptions.StorageException;
import se.sics.hop.metadata.persistence.ndb.ClusterjConnector;
import se.sics.hop.metadata.persistence.ndb.mysqlserver.CountHelper;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class LeaseClusterj extends LeaseDataAccess {

  @PersistenceCapable(table = TABLE_NAME)
  public interface LeaseDTO {

    @PrimaryKey
    @Column(name = HOLDER)
    String getHolder();

    void setHolder(String holder);

    @Column(name = LAST_UPDATE)
    long getLastUpdate();

    void setLastUpdate(long last_upd);

    @Column(name = HOLDER_ID)
    int getHolderId();

    void setHolderId(int holder_id);
  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public int countAll() throws StorageException {
    return CountHelper.countAll(TABLE_NAME);
  }

  @Override
  public HopLease findByPKey(String holder) throws StorageException {
    try {
      Session session = connector.obtainSession();
      LeaseDTO lTable = session.find(LeaseDTO.class, holder);
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

      dobj.where(dobj.get("holderId").equal(dobj.param("param")));

      Query<LeaseDTO> query = session.createQuery(dobj);
      query.setParameter("param", holderId); //the WHERE clause of SQL
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
      Query<LeaseDTO> query = session.createQuery(dobj);
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
      dobj.where(lessThan);
      Query query = session.createQuery(dobj);
      query.setParameter(param, new Long(timeLimit));
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void prepare(Collection<HopLease> removed, Collection<HopLease> newed, Collection<HopLease> modified) throws StorageException {
    try {
      Session session = connector.obtainSession();
      for (HopLease l : newed) {
        LeaseDTO lTable = session.newInstance(LeaseDTO.class);
        createPersistableLeaseInstance(l, lTable);
        session.savePersistent(lTable);
      }

      for (HopLease l : modified) {
        LeaseDTO lTable = session.newInstance(LeaseDTO.class);
        createPersistableLeaseInstance(l, lTable);
        session.savePersistent(lTable);
      }

      for (HopLease l : removed) {
        LeaseDTO lTable = session.newInstance(LeaseDTO.class, l.getHolder());
        session.deletePersistent(lTable);
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private SortedSet<HopLease> createList(List<LeaseDTO> list) {
    SortedSet<HopLease> finalSet = new TreeSet<HopLease>();
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
  }
}
