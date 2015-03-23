

package io.hops.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.hops.exception.StorageException;
import io.hops.metadata.yarn.entity.HopAppSchedulingInfoBlacklist;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.yarn.dal.AppSchedulingInfoBlacklistDataAccess;
import io.hops.metadata.yarn.tabledef.AppSchedulingInfoBlacklistTableDef;

public class AppSchedulingInfoBlacklistClusterJ implements AppSchedulingInfoBlacklistTableDef, AppSchedulingInfoBlacklistDataAccess<HopAppSchedulingInfoBlacklist>{

    
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
  public Map<String, List<HopAppSchedulingInfoBlacklist>> getAll() throws
        StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<AppSchedulingInfoBlacklistDTO> dobj
            = qb.createQueryDefinition(
                    AppSchedulingInfoBlacklistDTO.class);
    HopsQuery<AppSchedulingInfoBlacklistDTO> query = session.
            createQuery(dobj);
    List<AppSchedulingInfoBlacklistDTO> results = query.
            getResultList();
    return createMap(results);
  }
    
  @Override
  public void addAll(Collection<HopAppSchedulingInfoBlacklist> toAdd) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO> toPersist
            = new ArrayList<AppSchedulingInfoBlacklistDTO>();
    for (HopAppSchedulingInfoBlacklist hop : toAdd) {
      AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO persistable
              = createPersistable(hop, session);
      toPersist.add(persistable);
    }
    session.savePersistentAll(toPersist);
  }
  
  @Override
  public void removeAll(Collection<HopAppSchedulingInfoBlacklist> toRemove)
          throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO> toPersist
            = new ArrayList<AppSchedulingInfoBlacklistDTO>();
    for (HopAppSchedulingInfoBlacklist hop : toRemove) {
      Object[] objarr = new Object[2];
      objarr[0] = hop.getAppschedulinginfo_id();
      objarr[1] = hop.getBlacklisted();
      toPersist.add(session.newInstance(
              AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO.class,
              objarr));
    }
    session.deletePersistentAll(toPersist);
  }
    
    private HopAppSchedulingInfoBlacklist createHopAppSchedulingInfoBlacklist(AppSchedulingInfoBlacklistDTO appSchedulingInfoBlacklistDTO) {
        return new HopAppSchedulingInfoBlacklist(appSchedulingInfoBlacklistDTO.getappschedulinginfo_id(),
                                                appSchedulingInfoBlacklistDTO.getblacklisted());
    }

    private AppSchedulingInfoBlacklistDTO createPersistable(HopAppSchedulingInfoBlacklist hop, HopsSession session) throws StorageException {
        AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO appSchedulingInfoBlacklistDTO = session.newInstance(AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO.class);
        
        appSchedulingInfoBlacklistDTO.setappschedulinginfo_id(hop.getAppschedulinginfo_id());
        appSchedulingInfoBlacklistDTO.setblacklisted(hop.getBlacklisted());
        
        return appSchedulingInfoBlacklistDTO;
    }
    
    private List<HopAppSchedulingInfoBlacklist> createAppSchedulingInfoBlackList(List<AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO> results) {
        List<HopAppSchedulingInfoBlacklist> blackList = new ArrayList<HopAppSchedulingInfoBlacklist>();
        for (AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO persistable : results) {
            blackList.add(createHopAppSchedulingInfoBlacklist(persistable));
        }
        return blackList;
    }
 
   private Map<String, List<HopAppSchedulingInfoBlacklist>> createMap(
          List<AppSchedulingInfoBlacklistDTO> results) {
    Map<String, List<HopAppSchedulingInfoBlacklist>> map
            = new HashMap<String, List<HopAppSchedulingInfoBlacklist>>();
    for (AppSchedulingInfoBlacklistDTO dto : results) {
      HopAppSchedulingInfoBlacklist hop = createHopAppSchedulingInfoBlacklist(
              dto);
      if (map.get(hop.getAppschedulinginfo_id()) == null) {
        map.put(hop.getAppschedulinginfo_id(),
                new ArrayList<HopAppSchedulingInfoBlacklist>());
      }
      map.get(hop.getAppschedulinginfo_id()).add(hop);
    }
    return map;
  }
}
