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
import se.sics.hop.metadata.hdfs.entity.yarn.HopApplicationState;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.ApplicationStateDataAccess;
import se.sics.hop.metadata.yarn.tabledef.ApplicationStateTableDef;

/**
 *
 * @author nickstanogias
 */
public class ApplicationStateClusterJ implements ApplicationStateTableDef, ApplicationStateDataAccess<HopApplicationState> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface ApplicationStateDTO {

        @PrimaryKey
        @Column(name = APPLICATIONID)
        String getapplicationid();

        void setapplicationid(String applicationid);

        @Column(name = APPSTATE)
        byte[] getappstate();

        void setappstate(byte[] appstate);
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopApplicationState findByApplicationId(int id) throws StorageException {
        Session session = connector.obtainSession();

        ApplicationStateDTO appStateDTO = null;
        if (session != null) {
            appStateDTO = session.find(ApplicationStateClusterJ.ApplicationStateDTO.class, id);
        }
        if (appStateDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }
        return createHopApplicationState(appStateDTO);
    }

    @Override
    public List<HopApplicationState> getAll() throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();
            QueryDomainType<ApplicationStateDTO> dobj = qb.createQueryDefinition(ApplicationStateDTO.class);
            //Predicate pred1 = dobj.get("applicationid").equal(dobj.param("applicationid"));
            //dobj.where(pred1);
            Query<ApplicationStateDTO> query = session.createQuery(dobj);
            //query.setParameter("applicationid", applicationid);
            List<ApplicationStateDTO> results = query.getResultList();
            if (results != null && !results.isEmpty()) {
                return createHopApplicationStateList(results);
            } else {
                throw new StorageException("HOP :: Error retrieving ApplicationStates");
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }

    }

    @Override
    public void prepare(Collection<HopApplicationState> modified, Collection<HopApplicationState> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopApplicationState hop : removed) {
                    ApplicationStateDTO persistable = session.newInstance(ApplicationStateDTO.class, hop.getApplicationid());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopApplicationState hop : modified) {
                    ApplicationStateDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    private HopApplicationState createHopApplicationState(ApplicationStateDTO appStateDTO) {
        return new HopApplicationState(appStateDTO.getapplicationid(),
                appStateDTO.getappstate());
    }

    private List<HopApplicationState> createHopApplicationStateList(List<ApplicationStateDTO> list) {
        List<HopApplicationState> hopList = new ArrayList<HopApplicationState>();
        for (ApplicationStateDTO dto : list) {
            hopList.add(createHopApplicationState(dto));
        }
        return hopList;

    }

    private ApplicationStateDTO createPersistable(HopApplicationState hop, Session session) {
        ApplicationStateDTO appStateDTO = session.newInstance(ApplicationStateClusterJ.ApplicationStateDTO.class);
        appStateDTO.setapplicationid(hop.getApplicationid());
        appStateDTO.setappstate(hop.getAppstate());

        return appStateDTO;
    }
}
