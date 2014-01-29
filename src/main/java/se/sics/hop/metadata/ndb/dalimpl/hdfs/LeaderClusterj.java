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
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import se.sics.hop.metadata.hdfs.entity.hop.HopLeader;
import se.sics.hop.metadata.hdfs.dal.LeaderDataAccess;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.hdfs.tabledef.LeaderTableDef;

/**
 *
 * @author Salman <salman@sics.se>
 */
public class LeaderClusterj implements LeaderTableDef, LeaderDataAccess<HopLeader> {

  @PersistenceCapable(table = TABLE_NAME)
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

    @Column(name = AVG_REQUEST_PROCESSING_LATENCY)
    int getAvgRequestProcessingLatency();

    void setAvgRequestProcessingLatency(int avgRequestProcessingLatency);
  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public int countAll() throws StorageException {
    return findAll().size();
  }

  @Override
  public int countAllPredecessors(long id) throws StorageException {
    try {
      // TODO[Hooman]: code repetition. Use query for fetching "ids less than".
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType dobj = qb.createQueryDefinition(LeaderDTO.class);
      PredicateOperand propertyPredicate = dobj.get("id");
      String param = "id";
      PredicateOperand propertyLimit = dobj.param(param);
      Predicate lessThan = propertyPredicate.lessThan(propertyLimit);
      dobj.where(lessThan);
      Query query = session.createQuery(dobj);
      query.setParameter(param, new Long(id));
      return query.getResultList().size();
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public int countAllSuccessors(long id) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType dobj = qb.createQueryDefinition(LeaderDTO.class);
      PredicateOperand propertyPredicate = dobj.get("id");
      String param = "id";
      PredicateOperand propertyLimit = dobj.param(param);
      Predicate greaterThan = propertyPredicate.greaterThan(propertyLimit);
      dobj.where(greaterThan);
      Query query = session.createQuery(dobj);
      query.setParameter(param, new Long(id));
      return query.getResultList().size();
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

//    @Override
//    public Leader findById(long id) throws StorageException
//    {
//        try
//        {
//            Session session = connector.obtainSession();
//            LeaderDTO lTable = session.find(LeaderDTO.class, id);
//            if (lTable != null)
//            {
//                Leader leader = createLeader(lTable);
//                return leader;
//            }
//            return null;
//        } catch (Exception e)
//        {
//            throw new StorageException(e);
//        }
//    }
  @Override
  public HopLeader findByPkey(long id, int partitionKey) throws StorageException {

    try {
      Session session = connector.obtainSession();
      Object[] keys = new Object[]{id, partitionKey};
      LeaderDTO lTable = session.find(LeaderDTO.class, keys);
      if (lTable != null) {
        HopLeader leader = createLeader(lTable);
        return leader;
      }
      return null;
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public Collection<HopLeader> findAllByCounterGT(long counter) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType dobj = qb.createQueryDefinition(LeaderDTO.class);
      PredicateOperand propertyPredicate = dobj.get("counter");
      String param = "counter";
      PredicateOperand propertyLimit = dobj.param(param);
      Predicate greaterThan = propertyPredicate.greaterThan(propertyLimit);
      dobj.where(greaterThan);
      Query query = session.createQuery(dobj);
      query.setParameter(param, new Long(counter));
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public Collection<HopLeader> findAllByIDLT(long id) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType dobj = qb.createQueryDefinition(LeaderDTO.class);
      PredicateOperand propertyPredicate = dobj.get("id");
      String param = "id";
      PredicateOperand propertyLimit = dobj.param(param);
      Predicate greaterThan = propertyPredicate.lessThan(propertyLimit);
      dobj.where(greaterThan);
      Query query = session.createQuery(dobj);
      query.setParameter(param, new Long(id));
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public Collection<HopLeader> findAll() throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<LeaderDTO> dobj = qb.createQueryDefinition(LeaderDTO.class);
      Query<LeaderDTO> query = session.createQuery(dobj);
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void prepare(Collection<HopLeader> removed, Collection<HopLeader> newed, Collection<HopLeader> modified) throws StorageException {
    try {
      Session session = connector.obtainSession();
      for (HopLeader l : newed) {
        LeaderDTO lTable = session.newInstance(LeaderDTO.class);
        createPersistableLeaderInstance(l, lTable);
        session.savePersistent(lTable);
      }

      for (HopLeader l : modified) {
        LeaderDTO lTable = session.newInstance(LeaderDTO.class);
        createPersistableLeaderInstance(l, lTable);
        session.savePersistent(lTable);
      }

      for (HopLeader l : removed) {
        Object[] pk = new Object[2];
        pk[0] = l.getId();
        pk[1] = l.getPartitionVal();
        LeaderDTO lTable = session.newInstance(LeaderDTO.class, pk);
        session.deletePersistent(lTable);
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
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
            lTable.getAvgRequestProcessingLatency(),
            lTable.getPartitionVal());
  }

  private void createPersistableLeaderInstance(HopLeader leader, LeaderDTO lTable) {
    lTable.setId(leader.getId());
    lTable.setCounter(leader.getCounter());
    lTable.setHostname(leader.getHostName());
    lTable.setTimestamp(leader.getTimeStamp());
    lTable.setAvgRequestProcessingLatency(leader.getAvgRequestProcessingLatency());
    lTable.setPartitionVal(leader.getPartitionVal());
  }
}
