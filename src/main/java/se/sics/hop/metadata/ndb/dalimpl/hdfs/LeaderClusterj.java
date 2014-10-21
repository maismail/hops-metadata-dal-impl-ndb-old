package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.PredicateOperand;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import se.sics.hop.metadata.hdfs.entity.hop.HopLeader;
import se.sics.hop.metadata.hdfs.dal.LeaderDataAccess;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.hdfs.tabledef.LeaderTableDef;
import se.sics.hop.metadata.ndb.DBSession;

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
    try {
      // TODO[Hooman]: code repetition. Use query for fetching "ids less than".
      DBSession dbSession = connector.obtainSession();
      QueryBuilder qb = dbSession.getSession().getQueryBuilder();
      QueryDomainType dobj = qb.createQueryDefinition(LeaderDTO.class);
      PredicateOperand propertyPredicate = dobj.get("id");
      String param = "id";
      PredicateOperand propertyLimit = dobj.param(param);
      Predicate lessThan = propertyPredicate.lessThan(propertyLimit);
      dobj.where(lessThan);
      Query query = dbSession.getSession().createQuery(dobj);
      query.setParameter(param, new Long(id));
      return query.getResultList().size();
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public int countAllSuccessors(long id) throws StorageException {
    try {
      DBSession dbSession = connector.obtainSession();
      QueryBuilder qb = dbSession.getSession().getQueryBuilder();
      QueryDomainType dobj = qb.createQueryDefinition(LeaderDTO.class);
      PredicateOperand propertyPredicate = dobj.get("id");
      String param = "id";
      PredicateOperand propertyLimit = dobj.param(param);
      Predicate greaterThan = propertyPredicate.greaterThan(propertyLimit);
      dobj.where(greaterThan);
      Query query = dbSession.getSession().createQuery(dobj);
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
//            DBSession() dbSession.getSession() = connector.obtainSession();
//            LeaderDTO lTable = dbSession.getSession().find(LeaderDTO.class, id);
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
      DBSession dbSession = connector.obtainSession();
      Object[] keys = new Object[]{id, partitionKey};
      LeaderDTO lTable = dbSession.getSession().find(LeaderDTO.class, keys);
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
      DBSession dbSession = connector.obtainSession();
      QueryBuilder qb = dbSession.getSession().getQueryBuilder();
      QueryDomainType dobj = qb.createQueryDefinition(LeaderDTO.class);
      PredicateOperand propertyPredicate = dobj.get("counter");
      String param = "counter";
      PredicateOperand propertyLimit = dobj.param(param);
      Predicate greaterThan = propertyPredicate.greaterThan(propertyLimit);
      dobj.where(greaterThan);
      Query query = dbSession.getSession().createQuery(dobj);
      query.setParameter(param, new Long(counter));
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public Collection<HopLeader> findAllByIDLT(long id) throws StorageException {
    try {
      DBSession dbSession = connector.obtainSession();
      QueryBuilder qb = dbSession.getSession().getQueryBuilder();
      QueryDomainType dobj = qb.createQueryDefinition(LeaderDTO.class);
      PredicateOperand propertyPredicate = dobj.get("id");
      String param = "id";
      PredicateOperand propertyLimit = dobj.param(param);
      Predicate greaterThan = propertyPredicate.lessThan(propertyLimit);
      dobj.where(greaterThan);
      Query query = dbSession.getSession().createQuery(dobj);
      query.setParameter(param, new Long(id));
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public Collection<HopLeader> findAll() throws StorageException {
    try {
      DBSession dbSession = connector.obtainSession();
      QueryBuilder qb = dbSession.getSession().getQueryBuilder();
      QueryDomainType<LeaderDTO> dobj = qb.createQueryDefinition(LeaderDTO.class);
      Query<LeaderDTO> query = dbSession.getSession().createQuery(dobj);
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void prepare(Collection<HopLeader> removed, Collection<HopLeader> newed, Collection<HopLeader> modified) throws StorageException {
    try {
      DBSession dbSession = connector.obtainSession();
      List<LeaderDTO> changes = new ArrayList<LeaderDTO>();
      List<LeaderDTO> deletions = new ArrayList<LeaderDTO>();
      for (HopLeader l : newed) {
        LeaderDTO lTable = dbSession.getSession().newInstance(LeaderDTO.class);
        createPersistableLeaderInstance(l, lTable);
        changes.add(lTable);
      }

      for (HopLeader l : modified) {
        LeaderDTO lTable = dbSession.getSession().newInstance(LeaderDTO.class);
        createPersistableLeaderInstance(l, lTable);
        changes.add(lTable);
      }

      for (HopLeader l : removed) {
        LeaderDTO lTable = dbSession.getSession().newInstance(LeaderDTO.class);
        createPersistableLeaderInstance(l, lTable);
        deletions.add(lTable);
      }
      dbSession.getSession().deletePersistentAll(deletions);
      dbSession.getSession().savePersistentAll(changes);
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
