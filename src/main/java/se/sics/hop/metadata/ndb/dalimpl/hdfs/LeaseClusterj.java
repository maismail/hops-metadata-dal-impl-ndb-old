package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.sics.hop.metadata.hdfs.dal.LeaseDataAccess;
import se.sics.hop.metadata.hdfs.entity.hdfs.HopLease;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.mysqlserver.MySQLQueryHelper;
import se.sics.hop.metadata.hdfs.tabledef.LeaseTableDef;
import se.sics.hop.metadata.ndb.wrapper.HopsPredicate;
import se.sics.hop.metadata.ndb.wrapper.HopsPredicateOperand;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;

/**
 *
 * @author Hooman <hooman@sics.se>
 * @author salman <salman@sics.se>
 */
public class LeaseClusterj implements LeaseTableDef, LeaseDataAccess<HopLease> {

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
  public HopLease findByPKey(String holder) throws StorageException {
    HopsSession session = connector.obtainSession();
    Object[] key = new Object[2];
    key[0] = holder;
    key[1] = PART_KEY_VAL;
    LeaseDTO lTable = session.find(LeaseDTO.class, key);
    if (lTable != null) {
      HopLease lease = createLease(lTable);
      return lease;
    }
    return null;
  }

  @Override
  public HopLease findByHolderId(int holderId) throws StorageException {
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
      HopLease lease = createLease(leaseTables.get(0));
      return lease;
    } else {
      log.info("No rows found for holderID:" + holderId + " in Lease table");
      return null;
    }
  }

  @Override
  public Collection<HopLease> findAll() throws StorageException {
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
  public Collection<HopLease> findByTimeLimit(long timeLimit) throws StorageException {
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
  public void prepare(Collection<HopLease> removed, Collection<HopLease> newed, Collection<HopLease> modified) throws StorageException {
    HopsSession session = connector.obtainSession();
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
    HopsSession session = connector.obtainSession();
    session.deletePersistentAll(LeaseDTO.class);
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
