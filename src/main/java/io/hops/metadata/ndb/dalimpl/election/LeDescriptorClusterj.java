package io.hops.metadata.ndb.dalimpl.election;

import com.mysql.clusterj.annotation.PartitionKey;

import io.hops.exception.StorageException;
import io.hops.metadata.election.tabledef.LeDescriptorTableDef;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.election.dal.LeDescriptorDataAccess;
import io.hops.metadata.election.entity.LeDescriptor;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class LeDescriptorClusterj implements LeDescriptorTableDef, LeDescriptorDataAccess<LeDescriptor> {

  private ClusterjConnector connector = ClusterjConnector.getInstance();
  @PartitionKey(column = PARTITION_VAL)
  Class dto;

  public interface LeaderDTO {

    long getId();

    void setId(long id);

    int getPartitionVal();

    void setPartitionVal(int partitionVal);

    long getCounter();

    void setCounter(long counter);

    String getHostname();

    void setHostname(String hostname);

    String getHttpAddress();

    void setHttpAddress(String httpAddress);
  }

  public LeDescriptorClusterj(Class dto) {
    this.dto = dto;
  }

  @Override
  public LeDescriptor findByPkey(long id, int partitionKey) throws
      StorageException {
    HopsSession dbSession = connector.obtainSession();
    Object[] keys = new Object[]{partitionKey, id};
    LeaderDTO lTable =  (LeaderDTO)dbSession.find(dto, keys);
    if (lTable != null) {
      LeDescriptor leader = createDescriptor(lTable);
      return leader;
    }
    return null;
  }

  @Override
  public Collection<LeDescriptor> findAll() throws StorageException {
//    HopsSession dbSession = connector.obtainSession();
//    HopsQueryBuilder qb = dbSession.getQueryBuilder();
//    HopsQueryDomainType<LeaderDTO> dobj = qb.createQueryDefinition(LeaderDTO.class);
//    HopsPredicate pred1 = dobj.get("partitionVal").equal(dobj.param("partitionValParam"));
//    dobj.where(pred1);
//    HopsQuery<LeaderDTO> query = dbSession.createQuery(dobj);
//    query.setParameter("partitionValParam", 0);
//    return createList(query.getResultList());

    HopsSession dbSession = connector.obtainSession();
    HopsQueryBuilder qb = dbSession.getQueryBuilder();
    HopsQueryDomainType<LeaderDTO> dobj = qb.createQueryDefinition(dto);
    HopsQuery<LeaderDTO> query = dbSession.createQuery(dobj);
    return createList(query.getResultList());

  }

  @Override
  public void prepare(Collection<LeDescriptor> removed, Collection<LeDescriptor> newed, Collection<LeDescriptor> modified) throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    List<LeaderDTO> changes = new ArrayList<LeaderDTO>();
    List<LeaderDTO> deletions = new ArrayList<LeaderDTO>();
    for (LeDescriptor l : newed) {

      LeaderDTO lTable = (LeaderDTO)dbSession.newInstance(dto);
      createPersistableLeaderInstance(l, lTable);
      changes.add(lTable);
    }

    for (LeDescriptor l : modified) {
      LeaderDTO lTable = (LeaderDTO)dbSession.newInstance(dto);
      createPersistableLeaderInstance(l, lTable);
      changes.add(lTable);
    }

    for (LeDescriptor l : removed) {
      LeaderDTO lTable = (LeaderDTO)dbSession.newInstance(dto);
      createPersistableLeaderInstance(l, lTable);
      deletions.add(lTable);
    }
    dbSession.deletePersistentAll(deletions);
    dbSession.savePersistentAll(changes);
  }

  private Collection<LeDescriptor> createList(final List<LeaderDTO> list) {
    Collection<LeDescriptor> listRet = new ArrayList<LeDescriptor>(list.size());
    for (LeaderDTO dto : list) {
      listRet.add(createDescriptor(dto));
    }

    return listRet;
  }

  protected abstract LeDescriptor createDescriptor(LeaderDTO lTable);

  private void createPersistableLeaderInstance(LeDescriptor leader, LeaderDTO lTable) {
    lTable.setId(leader.getId());
    lTable.setCounter(leader.getCounter());
    lTable.setHostname(leader.getHostName());
    lTable.setHttpAddress(leader.getHttpAddress());
    lTable.setPartitionVal(leader.getPartitionVal());
  }
}
