package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.dal.LeaderDataAccess;
import se.sics.hop.metadata.hdfs.entity.hop.HopLeader;
import se.sics.hop.metadata.hdfs.tabledef.LeaderTableDef;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsPredicate;
import se.sics.hop.metadata.ndb.wrapper.HopsPredicateOperand;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 *
 * @author Salman <salman@sics.se>
 */
public class LeaderClusterj implements LeaderTableDef, LeaderDataAccess<HopLeader> {

  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column=PARTITION_VAL)
  public interface LeaderDTO {

    @PrimaryKey
    @Column(name = ID)
    long getId();

    void setId(long id);

    @PrimaryKey
    @Column(name = PARTITION_VAL)
    int getPartitionVal();

    void setPartitionVal(int partitionVal);

    @Column(name = COUNTER)
    long getCounter();

    void setCounter(long counter);

    @Column(name = TIMESTAMP)
    long getTimestamp();

    void setTimestamp(long timestamp);

    @Column(name = HOSTNAME)
    String getHostname();

    void setHostname(String hostname);

    @Column(name = HTTP_ADDRESS)
    String getHttpAddress();

    void setHttpAddress(String httpAddress);
  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public int countAll() throws StorageException {
    return findAll().size();
  }

  @Override
  public int countAllPredecessors(long id) throws StorageException {
    // TODO[Hooman]: code repetition. Use query for fetching "ids less than".
    HopsSession dbSession = connector.obtainSession();
    HopsQueryBuilder qb = dbSession.getQueryBuilder();
    HopsQueryDomainType dobj = qb.createQueryDefinition(LeaderDTO.class);
    HopsPredicateOperand propertyPredicate = dobj.get("id");
    String param = "id";
    HopsPredicateOperand propertyLimit = dobj.param(param);
    HopsPredicate lessThan = propertyPredicate.lessThan(propertyLimit);
    dobj.where(lessThan);
    HopsQuery query = dbSession.createQuery(dobj);
    query.setParameter(param, new Long(id));
    return query.getResultList().size();
  }

  @Override
  public int countAllSuccessors(long id) throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    HopsQueryBuilder qb = dbSession.getQueryBuilder();
    HopsQueryDomainType dobj = qb.createQueryDefinition(LeaderDTO.class);
    HopsPredicateOperand propertyPredicate = dobj.get("id");
    String param = "id";
    HopsPredicateOperand propertyLimit = dobj.param(param);
    HopsPredicate greaterThan = propertyPredicate.greaterThan(propertyLimit);
    dobj.where(greaterThan);
    HopsQuery query = dbSession.createQuery(dobj);
    query.setParameter(param, new Long(id));
    return query.getResultList().size();
  }

  @Override
  public HopLeader findByPkey(long id, int partitionKey) throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    Object[] keys = new Object[]{id, partitionKey};
    LeaderDTO lTable = dbSession.find(LeaderDTO.class, keys);
    if (lTable != null) {
      HopLeader leader = createLeader(lTable);
      return leader;
    }
    return null;
  }

  @Override
  public Collection<HopLeader> findAllByCounterGT(long counter) throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    HopsQueryBuilder qb = dbSession.getQueryBuilder();
    HopsQueryDomainType dobj = qb.createQueryDefinition(LeaderDTO.class);
    HopsPredicateOperand propertyPredicate = dobj.get("counter");
    String param = "counter";
    HopsPredicateOperand propertyLimit = dobj.param(param);
    HopsPredicate greaterThan = propertyPredicate.greaterThan(propertyLimit);
    dobj.where(greaterThan);
    HopsQuery query = dbSession.createQuery(dobj);
    query.setParameter(param, new Long(counter));
    return createList(query.getResultList());
  }

  @Override
  public Collection<HopLeader> findAllByIDLT(long id) throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    HopsQueryBuilder qb = dbSession.getQueryBuilder();
    HopsQueryDomainType dobj = qb.createQueryDefinition(LeaderDTO.class);
    HopsPredicateOperand propertyPredicate = dobj.get("id");
    String param = "id";
    HopsPredicateOperand propertyLimit = dobj.param(param);
    HopsPredicate greaterThan = propertyPredicate.lessThan(propertyLimit);
    dobj.where(greaterThan);
    HopsQuery query = dbSession.createQuery(dobj);
    query.setParameter(param, new Long(id));
    return createList(query.getResultList());
  }

  @Override
  public Collection<HopLeader> findAll() throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    HopsQueryBuilder qb = dbSession.getQueryBuilder();
    HopsQueryDomainType<LeaderDTO> dobj = qb.createQueryDefinition(LeaderDTO.class);
    HopsQuery<LeaderDTO> query = dbSession.createQuery(dobj);
    return createList(query.getResultList());
  }

  @Override
  public void prepare(Collection<HopLeader> removed, Collection<HopLeader> newed, Collection<HopLeader> modified) throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    List<LeaderDTO> changes = new ArrayList<LeaderDTO>();
    List<LeaderDTO> deletions = new ArrayList<LeaderDTO>();
    for (HopLeader l : newed) {
      LeaderDTO lTable = dbSession.newInstance(LeaderDTO.class);
      createPersistableLeaderInstance(l, lTable);
      changes.add(lTable);
    }

    for (HopLeader l : modified) {
      LeaderDTO lTable = dbSession.newInstance(LeaderDTO.class);
      createPersistableLeaderInstance(l, lTable);
      changes.add(lTable);
    }

    for (HopLeader l : removed) {
      LeaderDTO lTable = dbSession.newInstance(LeaderDTO.class);
      createPersistableLeaderInstance(l, lTable);
      deletions.add(lTable);
    }
    dbSession.deletePersistentAll(deletions);
    dbSession.savePersistentAll(changes);
  }

  private SortedSet<HopLeader> createList(List<LeaderDTO> list) {
    SortedSet<HopLeader> finalSet = new TreeSet<HopLeader>();
    for (LeaderDTO dto : list) {
      finalSet.add(createLeader(dto));
    }

    return finalSet;
  }

  private HopLeader createLeader(LeaderDTO lTable) {
    return new HopLeader(lTable.getId(),
        lTable.getCounter(),
        lTable.getTimestamp(),
        lTable.getHostname(),
        lTable.getHttpAddress(),
        lTable.getPartitionVal());
  }

  private void createPersistableLeaderInstance(HopLeader leader, LeaderDTO lTable) {
    lTable.setId(leader.getId());
    lTable.setCounter(leader.getCounter());
    lTable.setHostname(leader.getHostName());
    lTable.setTimestamp(leader.getTimeStamp());
    lTable.setHttpAddress(leader.getHttpAddress());
    lTable.setPartitionVal(leader.getPartitionVal());
  }
}
