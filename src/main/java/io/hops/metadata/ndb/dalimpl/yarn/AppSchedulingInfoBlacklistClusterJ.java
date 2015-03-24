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
import io.hops.metadata.yarn.dal.AppSchedulingInfoBlacklistDataAccess;
import io.hops.metadata.yarn.entity.AppSchedulingInfoBlacklist;
import io.hops.metadata.yarn.tabledef.AppSchedulingInfoBlacklistTableDef;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AppSchedulingInfoBlacklistClusterJ
    implements AppSchedulingInfoBlacklistTableDef,
    AppSchedulingInfoBlacklistDataAccess<AppSchedulingInfoBlacklist> {


  @PersistenceCapable(table = TABLE_NAME)
  public interface AppSchedulingInfoBlacklistDTO {

    @PrimaryKey
    @Column(name = APPSCHEDULINGINFO_ID)
    String getappschedulinginfo_id();

    void setappschedulinginfo_id(String appschedulinginfoid);

    @Column(name = BLACKLISTED)
    String getblacklisted();

    void setblacklisted(String blacklisted);

  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public Map<String, List<AppSchedulingInfoBlacklist>> getAll()
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<AppSchedulingInfoBlacklistDTO> dobj =
        qb.createQueryDefinition(AppSchedulingInfoBlacklistDTO.class);
    HopsQuery<AppSchedulingInfoBlacklistDTO> query = session.
        createQuery(dobj);
    List<AppSchedulingInfoBlacklistDTO> results = query.
        getResultList();
    return createMap(results);
  }

  @Override
  public void addAll(Collection<AppSchedulingInfoBlacklist> toAdd)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO>
        toPersist = new ArrayList<AppSchedulingInfoBlacklistDTO>();
    for (AppSchedulingInfoBlacklist hop : toAdd) {
      AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO
          persistable = createPersistable(hop, session);
      toPersist.add(persistable);
    }
    session.savePersistentAll(toPersist);
  }
  
  @Override
  public void removeAll(Collection<AppSchedulingInfoBlacklist> toRemove)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO>
        toPersist = new ArrayList<AppSchedulingInfoBlacklistDTO>();
    for (AppSchedulingInfoBlacklist hop : toRemove) {
      Object[] objarr = new Object[2];
      objarr[0] = hop.getAppschedulinginfo_id();
      objarr[1] = hop.getBlacklisted();
      toPersist.add(session.newInstance(
          AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO.class,
          objarr));
    }
    session.deletePersistentAll(toPersist);
  }

  private AppSchedulingInfoBlacklist createHopAppSchedulingInfoBlacklist(
      AppSchedulingInfoBlacklistDTO appSchedulingInfoBlacklistDTO) {
    return new AppSchedulingInfoBlacklist(
        appSchedulingInfoBlacklistDTO.getappschedulinginfo_id(),
        appSchedulingInfoBlacklistDTO.getblacklisted());
  }

  private AppSchedulingInfoBlacklistDTO createPersistable(
      AppSchedulingInfoBlacklist hop, HopsSession session)
      throws StorageException {
    AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO
        appSchedulingInfoBlacklistDTO = session.newInstance(
        AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO.class);

    appSchedulingInfoBlacklistDTO
        .setappschedulinginfo_id(hop.getAppschedulinginfo_id());
    appSchedulingInfoBlacklistDTO.setblacklisted(hop.getBlacklisted());

    return appSchedulingInfoBlacklistDTO;
  }

  private List<AppSchedulingInfoBlacklist> createAppSchedulingInfoBlackList(
      List<AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO> results) {
    List<AppSchedulingInfoBlacklist> blackList =
        new ArrayList<AppSchedulingInfoBlacklist>();
    for (AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO persistable : results) {
      blackList.add(createHopAppSchedulingInfoBlacklist(persistable));
    }
    return blackList;
  }

  private Map<String, List<AppSchedulingInfoBlacklist>> createMap(
      List<AppSchedulingInfoBlacklistDTO> results) {
    Map<String, List<AppSchedulingInfoBlacklist>> map =
        new HashMap<String, List<AppSchedulingInfoBlacklist>>();
    for (AppSchedulingInfoBlacklistDTO dto : results) {
      AppSchedulingInfoBlacklist hop = createHopAppSchedulingInfoBlacklist(dto);
      if (map.get(hop.getAppschedulinginfo_id()) == null) {
        map.put(hop.getAppschedulinginfo_id(),
            new ArrayList<AppSchedulingInfoBlacklist>());
      }
      map.get(hop.getAppschedulinginfo_id()).add(hop);
    }
    return map;
  }
}
