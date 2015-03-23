package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.tabledef.LeaseTableDef;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import io.hops.metadata.hdfs.dal.LeaseDataAccess;
import io.hops.metadata.hdfs.entity.Lease;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsPredicateOperand;
import io.hops.metadata.ndb.wrapper.HopsQuery;

public class LeaseClusterj implements LeaseTableDef, LeaseDataAccess<Lease> {

  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column=PART_KEY)
  
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
    @Index(name="update_idx")
    long getLastUpdate();
    void setLastUpdate(long last_upd);

    @Column(name = HOLDER_ID)
    @Index(name="holderid_idx")
    int getHolderId();
    void setHolderId(int holder_id);
  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();
   private static Log log = LogFactory.getLog(LeaseDataAccess.class);

  @Override
  public int countAll() throws StorageException {
    return MySQLQueryHelper.countAll(TABLE_NAME);
  }

  @Override
  public Lease findByPKey(String holder) throws StorageException {
    HopsSession session = connector.obtainSession();
    Object[] key = new Object[2];
    key[0] = holder;
    key[1] = PART_KEY_VAL;
    LeaseDTO lTable = session.find(LeaseDTO.class, key);
    if (lTable != null) {
      Lease lease = createLease(lTable);
      return lease;
    }
    return null;
  }

  @Override
  public Lease findByHolderId(int holderId) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<LeaseDTO> dobj = qb.createQueryDefinition(LeaseDTO.class);

    HopsPredicate pred1 = dobj.get("holderId").equal(dobj.param("param1"));
    HopsPredicate pred2 = dobj.get("partKey").equal(dobj.param("param2"));

    dobj.where(pred1/*.and(pred2)*/);

    HopsQuery<LeaseDTO> query = session.createQuery(dobj);
    query.setParameter("param1", holderId); //the WHERE clause of SQL
    query.setParameter("param2", PART_KEY_VAL);
    List<LeaseDTO> leaseTables = query.getResultList();

    if (leaseTables.size() > 1) {
      log.error("Error in selectLeaseTableInternal: Multiple rows with same holderID");
      return null;
    } else if (leaseTables.size() == 1) {
      Lease lease = createLease(leaseTables.get(0));
      return lease;
    } else {
      log.info("No rows found for holderID:" + holderId + " in Lease table");
      return null;
    }
  }

  @Override
  public Collection<Lease> findAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<LeaseDTO> dobj = qb.createQueryDefinition(LeaseDTO.class);
    HopsPredicate pred = dobj.get("partKey").equal(dobj.param("param"));
    dobj.where(pred);
    HopsQuery<LeaseDTO> query = session.createQuery(dobj);
    query.setParameter("param", PART_KEY_VAL);
    return createList(query.getResultList());
  }

  @Override
  public Collection<Lease> findByTimeLimit(long timeLimit) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType dobj = qb.createQueryDefinition(LeaseDTO.class);
    HopsPredicateOperand propertyPredicate = dobj.get("lastUpdate");
    String param = "timelimit";
    HopsPredicateOperand propertyLimit = dobj.param(param);
    HopsPredicate lessThan = propertyPredicate.lessThan(propertyLimit);
    dobj.where(lessThan.and(dobj.get("partKey").equal(dobj.param("partKeyParam"))));
    HopsQuery query = session.createQuery(dobj);
    query.setParameter(param, new Long(timeLimit));
    query.setParameter("partKeyParam", PART_KEY_VAL);
    return createList(query.getResultList());
  }

  @Override
  public void prepare(Collection<Lease> removed, Collection<Lease> newed, Collection<Lease> modified) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<LeaseDTO> changes = new ArrayList<LeaseDTO>();
    List<LeaseDTO> deletions = new ArrayList<LeaseDTO>();
    for (Lease l : newed) {
      LeaseDTO lTable = session.newInstance(LeaseDTO.class);
      createPersistableLeaseInstance(l, lTable);
      changes.add(lTable);
    }

    for (Lease l : modified) {
      LeaseDTO lTable = session.newInstance(LeaseDTO.class);
      createPersistableLeaseInstance(l, lTable);
      changes.add(lTable);
    }

    for (Lease l : removed) {
      Object[] key = new Object[2];
      key[0] = l.getHolder();
      key[1] = PART_KEY_VAL;
      LeaseDTO lTable = session.newInstance(LeaseDTO.class, key);
      deletions.add(lTable);
    }
    session.deletePersistentAll(deletions);
    session.savePersistentAll(changes);
  }

  private Collection<Lease> createList(List<LeaseDTO> list) {
    Collection<Lease> finalSet = new ArrayList<Lease>();
    for (LeaseDTO dto : list) {
      finalSet.add(createLease(dto));
    }

    return finalSet;
  }

  @Override
  public void removeAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    session.deletePersistentAll(LeaseDTO.class);
  }

  private Lease createLease(LeaseDTO lTable) {
    return new Lease(lTable.getHolder(), lTable.getHolderId(), lTable.getLastUpdate());
  }

  private void createPersistableLeaseInstance(Lease lease, LeaseDTO lTable) {
    lTable.setHolder(lease.getHolder());
    lTable.setHolderId(lease.getHolderId());
    lTable.setLastUpdate(lease.getLastUpdate());
    lTable.setPartKey(PART_KEY_VAL);
  }
}
