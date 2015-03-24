package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import se.sics.hop.metadata.hdfs.entity.hop.HopLeasePath;
import se.sics.hop.metadata.hdfs.dal.LeasePathDataAccess;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.hdfs.tabledef.LeasePathTableDef;
import se.sics.hop.metadata.ndb.wrapper.HopsPredicate;
import se.sics.hop.metadata.ndb.wrapper.HopsPredicateOperand;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;

/**
 * @author Hooman <hooman@sics.se>
 * @author Salman <salman@sics.se>
 */
public class LeasePathClusterj
    implements LeasePathTableDef, LeasePathDataAccess<HopLeasePath> {

  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column = PART_KEY)
  @Index(name = "holder_idx")
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
  public void prepare(Collection<HopLeasePath> removed,
      Collection<HopLeasePath> newed, Collection<HopLeasePath> modified)
      throws StorageException {
    List<LeasePathsDTO> changes = new ArrayList<LeasePathsDTO>();
    List<LeasePathsDTO> deletions = new ArrayList<LeasePathsDTO>();
    HopsSession dbSession = connector.obtainSession();
    for (HopLeasePath lp : newed) {
      LeasePathsDTO lTable = dbSession.newInstance(LeasePathsDTO.class);
      createPersistableLeasePathInstance(lp, lTable);
      changes.add(lTable);
    }

    for (HopLeasePath lp : modified) {
      LeasePathsDTO lTable = dbSession.newInstance(LeasePathsDTO.class);
      createPersistableLeasePathInstance(lp, lTable);
      changes.add(lTable);
    }

    for (HopLeasePath lp : removed) {
      Object[] key = new Object[2];
      key[0] = lp.getPath();
      key[1] = PART_KEY_VAL;
      LeasePathsDTO lTable = dbSession.newInstance(LeasePathsDTO.class, key);
      deletions.add(lTable);
    }
    dbSession.deletePersistentAll(deletions);
    dbSession.savePersistentAll(changes);
  }

  @Override
  public Collection<HopLeasePath> findByHolderId(int holderId)
      throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    HopsQueryBuilder qb = dbSession.getQueryBuilder();
    HopsQueryDomainType<LeasePathsDTO> dobj =
        qb.createQueryDefinition(LeasePathsDTO.class);
    HopsPredicate pred1 = dobj.get("holderId").equal(dobj.param("param1"));
    HopsPredicate pred2 = dobj.get("partKey").equal(dobj.param("param2"));
    dobj.where(pred1);
    HopsQuery<LeasePathsDTO> query = dbSession.createQuery(dobj);
    query.setParameter("param1", holderId);
    query.setParameter("param2", PART_KEY_VAL);
    return createList(query.getResultList());
  }

  @Override
  public HopLeasePath findByPKey(String path) throws StorageException {
    Object[] key = new Object[2];
    key[0] = path;
    key[1] = PART_KEY_VAL;
    HopsSession dbSession = connector.obtainSession();
    LeasePathsDTO lPTable = dbSession.find(LeasePathsDTO.class, key);
    HopLeasePath lPath = null;
    if (lPTable != null) {
      lPath = createLeasePath(lPTable);
    }
    return lPath;
  }

  @Override
  public Collection<HopLeasePath> findByPrefix(String prefix)
      throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    HopsQueryBuilder qb = dbSession.getQueryBuilder();
    HopsQueryDomainType dobj = qb.createQueryDefinition(LeasePathsDTO.class);
    HopsPredicateOperand propertyPredicate = dobj.get("path");
    String param = "prefix";
    HopsPredicateOperand propertyLimit = dobj.param(param);
    HopsPredicate like = propertyPredicate.like(propertyLimit)
        .and(dobj.get("partKey").equal(dobj.param("partKeyParam")));
    dobj.where(like);
    HopsQuery query = dbSession.createQuery(dobj);
    query.setParameter(param, prefix + "%");
    query.setParameter("partKeyParam", PART_KEY_VAL);
    return createList(query.getResultList());
  }

  @Override
  public Collection<HopLeasePath> findAll() throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    HopsQueryBuilder qb = dbSession.getQueryBuilder();
    HopsQueryDomainType dobj = qb.createQueryDefinition(LeasePathsDTO.class);
    HopsPredicate pred = dobj.get("partKey").equal(dobj.param("param"));
    dobj.where(pred);
    HopsQuery query = dbSession.createQuery(dobj);
    query.setParameter("param", PART_KEY_VAL);
    return createList(query.getResultList());
  }

  @Override
  public void removeAll() throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    dbSession.deletePersistentAll(LeasePathsDTO.class);
  }

  private List<HopLeasePath> createList(Collection<LeasePathsDTO> dtos) {
    List<HopLeasePath> list = new ArrayList<HopLeasePath>();
      if (dtos != null) {
          for (LeasePathsDTO leasePathsDTO : dtos) {
              list.add(createLeasePath(leasePathsDTO));
          }
    }
    
    return list;
  }

  private HopLeasePath createLeasePath(LeasePathsDTO leasePathTable) {
    return new HopLeasePath(leasePathTable.getPath(),
        leasePathTable.getHolderId());
  }

  private void createPersistableLeasePathInstance(HopLeasePath lp,
      LeasePathsDTO lTable) {
    lTable.setHolderId(lp.getHolderId());
    lTable.setPath(lp.getPath());
    lTable.setPartKey(PART_KEY_VAL);
  }
}
