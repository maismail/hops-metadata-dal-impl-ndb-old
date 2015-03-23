
package io.hops.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.entity.yarn.HopLoad;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.dal.RMLoadDataAccess;
import io.hops.metadata.yarn.tabledef.RMLoadTableDef;

public class RMLoadClusterJ implements RMLoadTableDef,
    RMLoadDataAccess<HopLoad> {

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
  public void update(HopLoad entry) throws StorageException {
    HopsSession session = connector.obtainSession();
    session.savePersistent(createPersistable(entry, session));
  }

  @Override
  public Map<String, HopLoad> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<RMLoadDTO> dobj
            = qb.createQueryDefinition(
                    RMLoadDTO.class);
    HopsQuery<RMLoadDTO> query = session.
            createQuery(dobj);
    List<RMLoadDTO> results = query.
            getResultList();
    return createMap(results);
  }

  private RMLoadDTO createPersistable(HopLoad entry, HopsSession session) throws
          StorageException {
    RMLoadDTO persistable = session.newInstance(RMLoadDTO.class);
    persistable.setrmhostname(entry.getRmHostName());
    persistable.setload(entry.getLoad());
    return persistable;
  }

  private Map<String, HopLoad> createMap(List<RMLoadDTO> results) {
    Map<String, HopLoad> map
            = new HashMap<String, HopLoad>();
    for (RMLoadDTO dto : results) {
      HopLoad hop = createHopLoad(dto);
      map.put(hop.getRmHostName(), hop);
    }
    return map;
  }

  private HopLoad createHopLoad(RMLoadDTO loadDTO) {
    if (loadDTO == null) {
      return null;
    }
    return new HopLoad(loadDTO.getrmhostname(), loadDTO.getload());
  }
}
