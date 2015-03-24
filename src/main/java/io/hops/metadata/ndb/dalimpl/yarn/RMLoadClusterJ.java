package io.hops.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.RMLoadDataAccess;
import io.hops.metadata.yarn.entity.Load;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RMLoadClusterJ implements TablesDef.RMLoadTableDef, RMLoadDataAccess<Load> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface RMLoadDTO {

    @PrimaryKey
    @Column(name = RMHOSTNAME)
    String getrmhostname();

    void setrmhostname(String rmhostname);

    @Column(name = LOAD)
    long getload();

    void setload(long load);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public void update(Load entry) throws StorageException {
    HopsSession session = connector.obtainSession();
    session.savePersistent(createPersistable(entry, session));
  }

  @Override
  public Map<String, Load> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<RMLoadDTO> dobj =
        qb.createQueryDefinition(RMLoadDTO.class);
    HopsQuery<RMLoadDTO> query = session.
        createQuery(dobj);
    List<RMLoadDTO> results = query.
        getResultList();
    return createMap(results);
  }

  private RMLoadDTO createPersistable(Load entry, HopsSession session)
      throws StorageException {
    RMLoadDTO persistable = session.newInstance(RMLoadDTO.class);
    persistable.setrmhostname(entry.getRmHostName());
    persistable.setload(entry.getLoad());
    return persistable;
  }

  private Map<String, Load> createMap(List<RMLoadDTO> results) {
    Map<String, Load> map = new HashMap<String, Load>();
    for (RMLoadDTO dto : results) {
      Load hop = createHopLoad(dto);
      map.put(hop.getRmHostName(), hop);
    }
    return map;
  }

  private Load createHopLoad(RMLoadDTO loadDTO) {
    if (loadDTO == null) {
      return null;
    }
    return new Load(loadDTO.getrmhostname(), loadDTO.getload());
  }
}
