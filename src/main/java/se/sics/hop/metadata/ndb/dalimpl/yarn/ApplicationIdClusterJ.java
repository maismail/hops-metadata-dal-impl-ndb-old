package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopApplicationId;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.ApplicationIdDataAccess;
import se.sics.hop.metadata.yarn.tabledef.ApplicationIdTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class ApplicationIdClusterJ implements ApplicationIdTableDef, ApplicationIdDataAccess<HopApplicationId> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface ApplicationIdDTO {

        @PrimaryKey
        @Column(name = ID)
        String getid();
        void setid(String id);
                
        @Column(name = APP_ID)
        int getappid();
        void setappid(int appid);

        @Column(name = CLUSTER_TIMESTAMP)
        long getclustertimestamp();
        void setclustertimestamp(long clustertimestamp);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopApplicationId findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        ApplicationIdDTO applicationIdDTO = null;
        if (session != null) {
            applicationIdDTO = session.find(ApplicationIdDTO.class, id);
        }
        if (applicationIdDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopApplicationId(applicationIdDTO);
    }

    @Override
    public HopApplicationId findByAppIdClusterTimestamp(int appid, long clustertimestamp) throws StorageException {
        Session session = connector.obtainSession();
        QueryBuilder qb = session.getQueryBuilder();

        QueryDomainType<ApplicationIdDTO> dobj = qb.createQueryDefinition(ApplicationIdDTO.class);
        Predicate pred1 = dobj.get("appid").equal(dobj.param("appid"));
        Predicate pred2 = dobj.get("clustertimestamp").equal(dobj.param("clustertimestamp"));
        pred1 = pred1.and(pred2);
        dobj.where(pred1);
        Query<ApplicationIdDTO> query = session.createQuery(dobj);
        query.setParameter("appid", appid);
        query.setParameter("clustertimestamp", clustertimestamp);
        List<ApplicationIdDTO> result = query.getResultList();

        if (result != null && !result.isEmpty()) {
            return createHopApplicationId(result.get(0));
        } else {
            throw new StorageException("HOP - Finished ApplicationId was not found");
        }
    }

    @Override
    public HopApplicationId findFinished(int id) throws StorageException {
        Session session = connector.obtainSession();
        QueryBuilder qb = session.getQueryBuilder();

        QueryDomainType<ApplicationIdDTO> dobj = qb.createQueryDefinition(ApplicationIdDTO.class);
        Predicate pred1 = dobj.get("id").equal(dobj.param("id"));
        Predicate pred2 = dobj.get("finished").equal(dobj.param("finished"));
        pred1 = pred1.and(pred2);
        dobj.where(pred1);
        Query<ApplicationIdDTO> query = session.createQuery(dobj);
        query.setParameter("id", id);
        query.setParameter("finished", 1);
        List<ApplicationIdDTO> result = query.getResultList();

        if (result != null && !result.isEmpty()) {
            return createHopApplicationId(result.get(0));
        } else {
            throw new StorageException("HOP - Finished ApplicationId was not found");
        }
    }

    @Override
    public List<HopApplicationId> findFinished() throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();

            QueryDomainType<ApplicationIdDTO> dobj = qb.createQueryDefinition(ApplicationIdDTO.class);
            Predicate pred1 = dobj.get("finished").equal(dobj.param("finished"));
            dobj.where(pred1);
            Query<ApplicationIdDTO> query = session.createQuery(dobj);
            query.setParameter("finished", 1);

            List<ApplicationIdDTO> results = query.getResultList();
            return createApplicationIdList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void prepare(Collection<HopApplicationId> modified, Collection<HopApplicationId> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopApplicationId hopApplicationId : removed) {

                    ApplicationIdDTO persistable = session.newInstance(ApplicationIdDTO.class, hopApplicationId.getId());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopApplicationId hopAppAttemptId : modified) {
                    ApplicationIdDTO persistable = createPersistable(hopAppAttemptId, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createApplicationId(HopApplicationId applicationId) throws StorageException {
        Session session = connector.obtainSession();
        createPersistable(applicationId, session);
    }

    @Override
    public void deleteAll() throws StorageException {
        Session session = connector.obtainSession();
        session.deletePersistentAll(ApplicationIdDTO.class);
    }

    @Override
    public void deleteFinished(List<HopApplicationId> list) throws StorageException {
        Session session = connector.obtainSession();
        List<ApplicationIdDTO> toRemove = new ArrayList<ApplicationIdDTO>();
        for (HopApplicationId hop : list) {
            ApplicationIdDTO appidDTO = session.newInstance(ApplicationIdDTO.class);
            appidDTO.setid(hop.getId());
            appidDTO.setappid(hop.getAppid());
            appidDTO.setclustertimestamp(hop.getClustertimestamp());
           // appidDTO.setfinished(hop.getFinished());
        }
        session.deletePersistentAll(toRemove);
    }

    @Override
    public List<HopApplicationId> findAll() throws StorageException {
        Session session = connector.obtainSession();
        QueryBuilder qb = session.getQueryBuilder();
        QueryDomainType<ApplicationIdDTO> dobj = qb.createQueryDefinition(ApplicationIdDTO.class);
        Query<ApplicationIdDTO> query = session.createQuery(dobj);
        List<ApplicationIdDTO> results = query.getResultList();
        try {
            return createApplicationIdList(results);
        } catch (IOException ex) {
            Logger.getLogger(ApplicationIdClusterJ.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    private List<HopApplicationId> createApplicationIdList(List<ApplicationIdDTO> list) throws IOException {
        List<HopApplicationId> applicationIds = new ArrayList<HopApplicationId>();
        for (ApplicationIdDTO persistable : list) {
            applicationIds.add(createHopApplicationId(persistable));
        }
        return applicationIds;
    }

    private ApplicationIdDTO createPersistable(HopApplicationId hopApplicationId, Session session) {
        ApplicationIdDTO applicationIdDTO = session.newInstance(ApplicationIdDTO.class);
        applicationIdDTO.setid(hopApplicationId.getId());
        applicationIdDTO.setappid(hopApplicationId.getAppid());
        applicationIdDTO.setclustertimestamp(hopApplicationId.getClustertimestamp());
        //applicationIdDTO.setfinished(hopApplicationId.getFinished());
        session.savePersistent(applicationIdDTO);
        return applicationIdDTO;
    }

    private HopApplicationId createHopApplicationId(ApplicationIdDTO applicationIdDTO) {
        HopApplicationId hop = new HopApplicationId(applicationIdDTO.getid(), applicationIdDTO.getappid(), applicationIdDTO.getclustertimestamp());
        return hop;
    }
}
