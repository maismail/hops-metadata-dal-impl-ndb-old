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
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopFinishedApplications;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.FinishedApplicationsDataAccess;
import se.sics.hop.metadata.yarn.tabledef.FinishedApplicationsTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class FinishedApplicationsClusterJ implements FinishedApplicationsTableDef, FinishedApplicationsDataAccess<HopFinishedApplications> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface FinishedApplicationsDTO {

        @PrimaryKey
        @Column(name = HOSTNAME)
        String gethostname();

        void sethostname(String hostname);

        @PrimaryKey
        @Column(name = COMMANDPORT)
        int getcommandport();

        void setcommandport(int commandport);

        @PrimaryKey
        @Column(name = APPLICATIONID)
        int getapplicationid();

        void setapplicationid(int applicationid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public List<HopFinishedApplications> findByRMNode(String hostname, int commandport) throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();

            QueryDomainType<FinishedApplicationsDTO> dobj = qb.createQueryDefinition(FinishedApplicationsDTO.class);
            Predicate pred1 = dobj.get("hostname").equal(dobj.param("hostname"));
            Predicate pred2 = dobj.get("commandport").equal(dobj.param("commandport"));
            pred1.and(pred2);
            dobj.where(pred1);

            Query<FinishedApplicationsDTO> query = session.createQuery(dobj);
            query.setParameter("hostname", hostname);
            query.setParameter("commandport", commandport);
            List<FinishedApplicationsDTO> results = query.getResultList();
            return createUpdatedContainerInfoList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public HopFinishedApplications findEntry(String hostname, int commandport, int applicationId) throws StorageException {
        Session session = connector.obtainSession();
        Object[] pk = new Object[3];
        pk[0] = hostname;
        pk[1] = commandport;
        pk[2] = applicationId;
        FinishedApplicationsDTO dto = session.find(FinishedApplicationsDTO.class, pk);
        if (dto == null) {
            throw new StorageException("Error while retrieving finishedapplication:" + hostname + "," + commandport + "," + applicationId);
        }
        return createHopFinishedApplications(dto);
    }

    @Override
    public void prepare(Collection<HopFinishedApplications> modified, Collection<HopFinishedApplications> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                List<FinishedApplicationsDTO> toRemove = new ArrayList<FinishedApplicationsDTO>();
                for (HopFinishedApplications entry : removed) {
                    toRemove.add(createPersistable(entry, session));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<FinishedApplicationsDTO> toModify = new ArrayList<FinishedApplicationsDTO>();
                for (HopFinishedApplications entry : modified) {
                    toModify.add(createPersistable(entry, session));
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException("Error while rmnode table:" + e.getMessage());
        }
    }

    private HopFinishedApplications createHopFinishedApplications(FinishedApplicationsDTO dto) {
        return new HopFinishedApplications(dto.gethostname(), dto.getcommandport(), dto.getapplicationid());
    }

    private FinishedApplicationsDTO createPersistable(HopFinishedApplications hop, Session session) {
        FinishedApplicationsDTO dto = session.newInstance(FinishedApplicationsDTO.class);
        dto.sethostname(hop.getHostname());
        dto.setcommandport(hop.getCommandport());
        dto.setapplicationid(hop.getApplicationId());
        return dto;
    }

    private List<HopFinishedApplications> createUpdatedContainerInfoList(List<FinishedApplicationsDTO> list) throws IOException {
        List<HopFinishedApplications> finishedApps = new ArrayList<HopFinishedApplications>();
        for (FinishedApplicationsDTO persistable : list) {
            finishedApps.add(createHopFinishedApplications(persistable));
        }
        return finishedApps;
    }
}
