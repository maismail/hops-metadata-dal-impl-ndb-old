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
import se.sics.hop.metadata.hdfs.entity.yarn.HopApplicationAttemptId;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.ApplicationAttemptIdDataAccess;
import se.sics.hop.metadata.yarn.tabledef.ApplicationAttemptIdTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class ApplicationAttemptIdClusterJ implements ApplicationAttemptIdTableDef, ApplicationAttemptIdDataAccess<HopApplicationAttemptId> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface ApplicationAttemptIdDTO {

        @PrimaryKey

        @Column(name = ATTEMPTID)
        String getattemptid();
        void setattemptid(String attemptid);

        @Column(name = APPID)
        String getappid();

        void setappid(String appid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public List<HopApplicationAttemptId> findByAttemptId(String attemptid) throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();

            QueryDomainType<ApplicationAttemptIdClusterJ.ApplicationAttemptIdDTO> dobj = qb.createQueryDefinition(ApplicationAttemptIdClusterJ.ApplicationAttemptIdDTO.class);
            Predicate pred1 = dobj.get("attemptid").equal(dobj.param("attemptid"));
            dobj.where(pred1);

            Query<ApplicationAttemptIdClusterJ.ApplicationAttemptIdDTO> query = session.createQuery(dobj);
            query.setParameter("attemptid", attemptid);
            List<ApplicationAttemptIdClusterJ.ApplicationAttemptIdDTO> results = query.getResultList();
            return createApplicationAttemptIdList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    @Override
    public List<HopApplicationAttemptId> findByAppId(String appid) throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();

            QueryDomainType<ApplicationAttemptIdClusterJ.ApplicationAttemptIdDTO> dobj = qb.createQueryDefinition(ApplicationAttemptIdClusterJ.ApplicationAttemptIdDTO.class);
            Predicate pred1 = dobj.get("appid").equal(dobj.param("appid"));
            dobj.where(pred1);

            Query<ApplicationAttemptIdClusterJ.ApplicationAttemptIdDTO> query = session.createQuery(dobj);
            query.setParameter("appid", appid);
            List<ApplicationAttemptIdClusterJ.ApplicationAttemptIdDTO> results = query.getResultList();
            return createApplicationAttemptIdList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    @Override
    public List<HopApplicationAttemptId> findAll() throws StorageException {
        Session session = connector.obtainSession();
        QueryBuilder qb = session.getQueryBuilder();
        QueryDomainType<ApplicationAttemptIdClusterJ.ApplicationAttemptIdDTO> dobj = qb.createQueryDefinition(ApplicationAttemptIdClusterJ.ApplicationAttemptIdDTO.class);
        Query<ApplicationAttemptIdClusterJ.ApplicationAttemptIdDTO> query = session.createQuery(dobj);
        List<ApplicationAttemptIdClusterJ.ApplicationAttemptIdDTO> results = query.getResultList();
        try {
            return createApplicationAttemptIdList(results);
        } catch (IOException ex) {
            Logger.getLogger(ApplicationAttemptIdClusterJ.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    private List<HopApplicationAttemptId> createApplicationAttemptIdList(List<ApplicationAttemptIdClusterJ.ApplicationAttemptIdDTO> list) throws IOException {
        List<HopApplicationAttemptId> applicationAttemptIds = new ArrayList<HopApplicationAttemptId>();
        for (ApplicationAttemptIdClusterJ.ApplicationAttemptIdDTO persistable : list) {
            applicationAttemptIds.add(createHopApplicationAttemptId(persistable));
        }
        return applicationAttemptIds;
    }
    
    @Override
    public HopApplicationAttemptId findByAttemptIdAppId(int attemptId, int appId) throws StorageException {
         ApplicationAttemptIdClusterJ.ApplicationAttemptIdDTO applicationAttemptIdDTO = null;
        try {
            Session session = connector.obtainSession();

            Object[] objarr = new Object[2];
            objarr[0] = attemptId;
            objarr[1] = appId;

            if (session != null) {
                applicationAttemptIdDTO = session.find(ApplicationAttemptIdClusterJ.ApplicationAttemptIdDTO.class, objarr);
            }
            if (applicationAttemptIdDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row:" + attemptId + ":" + appId);
            }
            return createHopApplicationAttemptId(applicationAttemptIdDTO);
        } catch (Exception e) {
            if (e.getMessage().contains("Tuple did not exist")) {
                throw new StorageException("HOP :: Error while retrieving row:" + attemptId + ":" + appId);
            }
        }
        throw new StorageException("HOP :: Error while retrieving row:" + attemptId + ":" + appId);
    }

    @Override
    public void prepare(Collection<HopApplicationAttemptId> modified, Collection<HopApplicationAttemptId> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopApplicationAttemptId hopAppAttemptId : removed) {

                    ApplicationAttemptIdDTO persistable = session.newInstance(ApplicationAttemptIdDTO.class, hopAppAttemptId.getAttemptId());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopApplicationAttemptId hopAppAttemptId : modified) {
                    ApplicationAttemptIdDTO persistable = createPersistable(hopAppAttemptId, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createApplicationAttemptId(HopApplicationAttemptId applicationattemptid) throws StorageException {
        Session session = connector.obtainSession();
        createPersistable(applicationattemptid, session);
    }

    private ApplicationAttemptIdDTO createPersistable(HopApplicationAttemptId hopAppAttemptId, Session session) {
        ApplicationAttemptIdDTO appAttemptIdDTO = session.newInstance(ApplicationAttemptIdDTO.class);
        appAttemptIdDTO.setappid(hopAppAttemptId.getAppid());
        appAttemptIdDTO.setattemptid(hopAppAttemptId.getAttemptId());
        session.savePersistent(appAttemptIdDTO);
        return appAttemptIdDTO;
    }

    private HopApplicationAttemptId createHopApplicationAttemptId(ApplicationAttemptIdDTO appAttemptIdDTO) {
        HopApplicationAttemptId hop = new HopApplicationAttemptId(appAttemptIdDTO.getattemptid(), appAttemptIdDTO.getappid());
        return hop;
    }
}
