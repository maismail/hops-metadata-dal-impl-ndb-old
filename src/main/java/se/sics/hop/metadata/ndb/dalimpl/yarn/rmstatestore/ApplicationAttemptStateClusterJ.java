package se.sics.hop.metadata.ndb.dalimpl.yarn.rmstatestore;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.rmstatestore.HopApplicationAttemptState;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.rmstatestore.ApplicationAttemptStateDataAccess;
import se.sics.hop.metadata.yarn.tabledef.rmstatestore.ApplicationAttemptStateTableDef;
import static se.sics.hop.metadata.yarn.tabledef.rmstatestore.ApplicationAttemptStateTableDef.APPLICATIONATTEMPTID;
import static se.sics.hop.metadata.yarn.tabledef.rmstatestore.ApplicationAttemptStateTableDef.APPLICATIONATTEMPTSTATE;

/**
 *
 * @author nickstanogias
 */
public class ApplicationAttemptStateClusterJ implements ApplicationAttemptStateTableDef, ApplicationAttemptStateDataAccess<HopApplicationAttemptState> {

    @PersistenceCapable(table = TABLE_NAME)
  public interface ApplicationAttemptStateDTO {

    @PrimaryKey
    @Column(name = APPLICATIONID)
    String getapplicationid();

    void setapplicationid(String applicationid);

    @Column(name = APPLICATIONATTEMPTID)
    String getapplicationattemptid();

    void setapplicationattemptid(String applicationattemptid);

    @Column(name = APPLICATIONATTEMPTSTATE)
    byte[] getapplicationattemptstate();

    void setapplicationattemptstate(byte[] applicationattemptstate);

    @Column(name = HOST)
    String getapplicationattempthost();

    void setapplicationattempthost(String host);

    @Column(name = RPCPORT)
    int getapplicationattemptrpcport();

    void setapplicationattemptrpcport(int port);

    @Column(name = TOKENS)
    byte[] getapplicationattempttokens();

    void setapplicationattempttokens(byte[] tokens);

    @Column(name = TRAKINGURL)
    String getapplicationattempttrakingurl();

    void setapplicationattempttrakingurl(String trakingUrl);
  }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopApplicationAttemptState findEntry(int applicationid, int applicationattemptid) throws StorageException {
        Session session = connector.obtainSession();
        Object[] objarr = new Object[2];
        objarr[0] = applicationid;
        objarr[1] = applicationattemptid;
        ApplicationAttemptStateClusterJ.ApplicationAttemptStateDTO entry = null;
        if (session != null) {
            entry = session.find(ApplicationAttemptStateClusterJ.ApplicationAttemptStateDTO.class, objarr);
        }
        if (entry == null) {
            throw new StorageException("HOP :: Error while retrieving applicationAttemptState with id" + applicationid);
        }

        return createHopApplicationAttemptState(entry);
    }

    @Override
    public List<HopApplicationAttemptState> findApplicationAttemptIdByApplicationId(String applicationid) throws StorageException {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();
            QueryDomainType<ApplicationAttemptStateDTO> dobj = qb.createQueryDefinition(ApplicationAttemptStateDTO.class);
            Predicate pred1 = dobj.get("applicationid").equal(dobj.param("applicationid"));
            dobj.where(pred1);
            Query<ApplicationAttemptStateDTO> query = session.createQuery(dobj);
            query.setParameter("applicationid", applicationid);
            List<ApplicationAttemptStateDTO> results = query.getResultList();
            List<HopApplicationAttemptState> appAttemptIds;
            if (results != null && !results.isEmpty()) {
                appAttemptIds = new ArrayList<HopApplicationAttemptState>();
                for (ApplicationAttemptStateDTO dto : results) {
                    appAttemptIds.add(createHopApplicationAttemptState(dto));
                }
                return appAttemptIds;
            } else {
                return null;
            }
    }
    
  @Override
  public Map<String, List<HopApplicationAttemptState>> getAll() throws
          StorageException {
    Session session = connector.obtainSession();
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<ApplicationAttemptStateDTO> dobj = qb.createQueryDefinition(
            ApplicationAttemptStateDTO.class);
    Query<ApplicationAttemptStateDTO> query = session.createQuery(dobj);
    List<ApplicationAttemptStateDTO> results = query.getResultList();

    return createMap(results);
  }

    @Override
    public void createApplicationAttemptStateEntry(HopApplicationAttemptState entry) throws StorageException {
        Session session = connector.obtainSession();
        session.savePersistent(createPersistable(entry, session));
    }

    @Override
    public void prepare(Collection<HopApplicationAttemptState> modified, Collection<HopApplicationAttemptState> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                List<ApplicationAttemptStateDTO> toRemove = new ArrayList<ApplicationAttemptStateDTO>();
                for (HopApplicationAttemptState hop : removed) {
                    Object[] objarr = new Object[2];
                    objarr[0] = hop.getApplicationId();
                    objarr[1] = hop.getApplicationattemptid();
                    toRemove.add(session.newInstance(ApplicationAttemptStateClusterJ.ApplicationAttemptStateDTO.class, objarr));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<ApplicationAttemptStateDTO> toModify = new ArrayList<ApplicationAttemptStateDTO>();
                for (HopApplicationAttemptState hop : modified) {
                    toModify.add(createPersistable(hop, session));
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

  private HopApplicationAttemptState createHopApplicationAttemptState(
          ApplicationAttemptStateDTO entry) {
    ByteBuffer buffer;
    if (entry.getapplicationattempttokens() != null) {
      buffer = ByteBuffer.wrap(entry.getapplicationattempttokens());
    } else {
      buffer = null;
    }
    return new HopApplicationAttemptState(entry.getapplicationid(),
            entry.getapplicationattemptid(),
            entry.getapplicationattemptstate(),
            entry.getapplicationattempthost(),
            entry.getapplicationattemptrpcport(),
            buffer,
            entry.getapplicationattempttrakingurl());
  }

    private List<HopApplicationAttemptState> createHopApplicationAttemptStateList(List<ApplicationAttemptStateDTO> list) {
        List<HopApplicationAttemptState> hopList = new ArrayList<HopApplicationAttemptState>();
        for (ApplicationAttemptStateDTO dto : list) {
            hopList.add(createHopApplicationAttemptState(dto));
        }
        return hopList;

    }

  private ApplicationAttemptStateDTO createPersistable(
          HopApplicationAttemptState hop, Session session) {
    ApplicationAttemptStateClusterJ.ApplicationAttemptStateDTO applicationAttemptStateDTO
            = session.newInstance(
                    ApplicationAttemptStateClusterJ.ApplicationAttemptStateDTO.class);

    applicationAttemptStateDTO.setapplicationid(hop.getApplicationId());
    applicationAttemptStateDTO.setapplicationattemptid(hop.
            getApplicationattemptid());
    applicationAttemptStateDTO.setapplicationattemptstate(hop.
            getApplicationattemptstate());
    applicationAttemptStateDTO.setapplicationattempthost(hop.getHost());
    applicationAttemptStateDTO.setapplicationattemptrpcport(hop.getRpcPort());
    if (hop.getAppAttemptTokens() != null) {
      applicationAttemptStateDTO.setapplicationattempttokens(hop.
              getAppAttemptTokens().array());
    }
    applicationAttemptStateDTO.setapplicationattempttrakingurl(hop.getUrl());
    return applicationAttemptStateDTO;
  }
  
  private Map<String, List<HopApplicationAttemptState>>
          createMap(List<ApplicationAttemptStateDTO> results) {
    Map<String, List<HopApplicationAttemptState>> map
            = new HashMap<String, List<HopApplicationAttemptState>>();
    for (ApplicationAttemptStateDTO persistable : results) {
      HopApplicationAttemptState hop
              = createHopApplicationAttemptState(persistable);
      if (map.get(hop.getApplicationId()) == null) {
        map.put(hop.getApplicationId(),
                new ArrayList<HopApplicationAttemptState>());
      }
      map.get(hop.getApplicationId()).add(hop);
    }
    return map;
  }
}
