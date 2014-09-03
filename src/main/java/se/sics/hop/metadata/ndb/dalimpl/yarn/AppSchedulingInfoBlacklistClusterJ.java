/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopAppSchedulingInfoBlacklist;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.AppSchedulingInfoBlacklistDataAccess;
import se.sics.hop.metadata.yarn.tabledef.AppSchedulingInfoBlacklistTableDef;

/**
 *
 * @author nickstanogias
 */
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
    public List<HopAppSchedulingInfoBlacklist> findById(String id) throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();

            QueryDomainType<AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO> dobj = qb.createQueryDefinition(AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO.class);
            Predicate pred1 = dobj.get("appschedulinginfo_id").equal(dobj.param("appschedulinginfo_id"));
            dobj.where(pred1);
            Query<AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO> query = session.createQuery(dobj);
            query.setParameter("appschedulinginfo_id", id);

            List<AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO> results = query.getResultList();
            return createAppSchedulingInfoBlackList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void prepare(Collection<HopAppSchedulingInfoBlacklist> modified, Collection<HopAppSchedulingInfoBlacklist> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                List<AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO> toRemove = new ArrayList<AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO>();
                for (HopAppSchedulingInfoBlacklist hop : removed) {
                    Object[] objarr = new Object[2];
                    objarr[0] = hop.getAppschedulinginfo_id();
                    objarr[1] = hop.getBlacklisted();
                    toRemove.add(session.newInstance(AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO.class, objarr));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                for (HopAppSchedulingInfoBlacklist hop : modified) {
                    AppSchedulingInfoBlacklistClusterJ.AppSchedulingInfoBlacklistDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    private HopAppSchedulingInfoBlacklist createHopAppSchedulingInfoBlacklist(AppSchedulingInfoBlacklistDTO appSchedulingInfoBlacklistDTO) {
        return new HopAppSchedulingInfoBlacklist(appSchedulingInfoBlacklistDTO.getappschedulinginfo_id(),
                                                appSchedulingInfoBlacklistDTO.getblacklisted());
    }

    private AppSchedulingInfoBlacklistDTO createPersistable(HopAppSchedulingInfoBlacklist hop, Session session) {
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
    
}
