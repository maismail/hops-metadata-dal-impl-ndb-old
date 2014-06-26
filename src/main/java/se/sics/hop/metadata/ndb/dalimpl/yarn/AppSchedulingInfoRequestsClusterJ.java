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
import se.sics.hop.metadata.hdfs.entity.yarn.HopAppSchedulingInfoRequests;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.AppSchedulingInfoRequestsDataAccess;
import se.sics.hop.metadata.yarn.tabledef.AppSchedulingInfoRequestsTableDef;

/**
 *
 * @author nickstanogias
 */
public class AppSchedulingInfoRequestsClusterJ implements AppSchedulingInfoRequestsTableDef, AppSchedulingInfoRequestsDataAccess<HopAppSchedulingInfoRequests>{


    @PersistenceCapable(table = TABLE_NAME)
    public interface AppSchedulingInfoRequestsDTO {

        @PrimaryKey
        @Column(name = APPSCHEDULINGINFO_ID)
        int getappschedulinginfoid();
        void setappschedulinginfoid(int appschedulinginfoid);

        @Column(name = PRIORITY_ID)
        int getpriorityid();
        void setpriorityid(int priorityid);
        
        @Column(name = NAME)
        String getname();
        void setname(String name);
        
        @Column(name = RESOURCEREQUEST_ID)
        int getresourcerequestid();
        void setresourcerequestid(int resourcerequestid);
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
     
    @Override
    public HopAppSchedulingInfoRequests findEntry(int appSchedulingInfoId, int priorityId, String name) throws StorageException {
        Session session = connector.obtainSession();
        Object[] objarr = new Object[3];
        objarr[0] = appSchedulingInfoId;
        objarr[1] = priorityId;
        objarr[2] = name;
        AppSchedulingInfoRequestsDTO entry = null;
        if (session != null) {
            entry = session.find(AppSchedulingInfoRequestsDTO.class, objarr);
        }
        if (entry == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }
        
        return createAppSchedulingInfoRequests(entry);
    }

    @Override
    public void prepare(Collection<HopAppSchedulingInfoRequests> modified, Collection<HopAppSchedulingInfoRequests> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopAppSchedulingInfoRequests hop : removed) {
                    Object[] objarr = new Object[3];
                    objarr[0] = hop.getAppschedulinginfo_id();
                    objarr[1] = hop.getPriority_id();
                    objarr[2] = hop.getName();
                    AppSchedulingInfoRequestsDTO persistable = session.newInstance(AppSchedulingInfoRequestsDTO.class, objarr);
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopAppSchedulingInfoRequests hop : modified) {
                    AppSchedulingInfoRequestsDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createAppSchedulingInfoRequestsEntry(HopAppSchedulingInfoRequests entry) throws StorageException {
        Session session = connector.obtainSession();
        session.savePersistent(createPersistable(entry, session));
    }

    @Override
    public List<HopAppSchedulingInfoRequests> getAllByAppSchedulingInfoId(int appSchedulingInfoId) throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();

            QueryDomainType<AppSchedulingInfoRequestsClusterJ.AppSchedulingInfoRequestsDTO> dobj = qb.createQueryDefinition(AppSchedulingInfoRequestsClusterJ.AppSchedulingInfoRequestsDTO.class);
            Predicate pred1 = dobj.get("appschedulinginfoid").equal(dobj.param("appschedulinginfoid"));
            dobj.where(pred1);
            Query<AppSchedulingInfoRequestsClusterJ.AppSchedulingInfoRequestsDTO> query = session.createQuery(dobj);
            query.setParameter("appschedulinginfoid", appSchedulingInfoId);

            List<AppSchedulingInfoRequestsClusterJ.AppSchedulingInfoRequestsDTO> results = query.getResultList();
            return createFifoSchedulerAppsList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    private HopAppSchedulingInfoRequests createAppSchedulingInfoRequests(AppSchedulingInfoRequestsDTO entry) {
        return new HopAppSchedulingInfoRequests(entry.getappschedulinginfoid(),
                                                entry.getpriorityid(),
                                                entry.getname(),
                                                entry.getresourcerequestid());
    }

    private AppSchedulingInfoRequestsDTO createPersistable(HopAppSchedulingInfoRequests hop, Session session) {
        AppSchedulingInfoRequestsClusterJ.AppSchedulingInfoRequestsDTO appSchedulingInfoRequestsDTO = session.newInstance(AppSchedulingInfoRequestsClusterJ.AppSchedulingInfoRequestsDTO.class);
        
        appSchedulingInfoRequestsDTO.setappschedulinginfoid(hop.getAppschedulinginfo_id());
        appSchedulingInfoRequestsDTO.setpriorityid(hop.getPriority_id());
        appSchedulingInfoRequestsDTO.setname(hop.getName());
        appSchedulingInfoRequestsDTO.setresourcerequestid(hop.getResourcerequest_id());
        
        return appSchedulingInfoRequestsDTO;
    }

    private List<HopAppSchedulingInfoRequests> createFifoSchedulerAppsList(List<AppSchedulingInfoRequestsDTO> results) {
        List<HopAppSchedulingInfoRequests> appSchedulingInfoRequests = new ArrayList<HopAppSchedulingInfoRequests>();
        for (AppSchedulingInfoRequestsClusterJ.AppSchedulingInfoRequestsDTO persistable : results) {
            appSchedulingInfoRequests.add(createAppSchedulingInfoRequests(persistable));
        }
        return appSchedulingInfoRequests;
    }
}
