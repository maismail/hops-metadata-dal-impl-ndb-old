package io.hops.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.hops.metadata.yarn.entity.HopNextHeartbeat;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.yarn.dal.NextHeartbeatDataAccess;
import io.hops.metadata.yarn.tabledef.NextHeartbeatTableDef;

public class NextHeartbeatClusterJ implements NextHeartbeatTableDef,
        NextHeartbeatDataAccess<HopNextHeartbeat> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface NextHeartbeatDTO extends RMNodeComponentDTO {

    @PrimaryKey
    @Column(name = RMNODEID)
    String getrmnodeid();

    void setrmnodeid(String rmnodeid);

    @Column(name = NEXTHEARTBEAT)
    int getNextheartbeat();

    void setNextheartbeat(int Nextheartbeat);

  }
  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public Map<String, Boolean> getAll() throws StorageException {

    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<NextHeartbeatDTO> dobj = qb.createQueryDefinition(
            NextHeartbeatDTO.class);
    HopsQuery<NextHeartbeatDTO> query = session.createQuery(dobj);
    List<NextHeartbeatDTO> results = query.getResultList();

    return createMap(results);
  }

  @Override
  public boolean findEntry(String rmnodeId)
          throws StorageException {
    HopsSession session = connector.obtainSession();
    NextHeartbeatDTO nextHBDTO = session.find(NextHeartbeatDTO.class, rmnodeId);
    if (nextHBDTO != null) {
      return createHopNextHeartbeat(nextHBDTO).isNextheartbeat();
    }
    return false;
  }

  @Override
  public void updateNextHeartbeat(String rmnodeid, boolean nextHeartbeat)
          throws StorageException {
    HopsSession session = connector.obtainSession();
    session.savePersistent(createPersistable(new HopNextHeartbeat(rmnodeid,
            nextHeartbeat), session));
    session.flush();
  }

  private NextHeartbeatDTO createPersistable(
          HopNextHeartbeat hopNextHeartbeat, HopsSession session) throws
          StorageException {
    NextHeartbeatDTO DTO = session.newInstance(NextHeartbeatDTO.class);
    //Set values to persist new persistedEvent
    DTO.setrmnodeid(hopNextHeartbeat.getRmnodeid());
    DTO.setNextheartbeat(booleanToInt(hopNextHeartbeat.isNextheartbeat()));
    return DTO;
  }

  public static HopNextHeartbeat createHopNextHeartbeat(
          NextHeartbeatDTO nextHBDTO) {
    return new HopNextHeartbeat(nextHBDTO.getrmnodeid(), intToBoolean(nextHBDTO.
            getNextheartbeat()));
  }

  private Map<String, Boolean> createMap(List<NextHeartbeatDTO> results) {
    Map<String, Boolean> map = new HashMap<String, Boolean>();
    for (NextHeartbeatDTO persistable : results) {
      map.put(persistable.getrmnodeid(), intToBoolean(persistable.
              getNextheartbeat()));
    }
    return map;
  }

  /**
   * As ClusterJ boolean is buggy, we use Int to store the boolean field to NDB
   * and we convert it here to integer.
   * <p>
   * @return
   */
  private static boolean intToBoolean(int a) {
    return a == NEXTHEARTBEAT_TRUE;
  }

  private int booleanToInt(boolean a) {
    return a ? NEXTHEARTBEAT_TRUE : NEXTHEARTBEAT_FALSE;
  }

}
