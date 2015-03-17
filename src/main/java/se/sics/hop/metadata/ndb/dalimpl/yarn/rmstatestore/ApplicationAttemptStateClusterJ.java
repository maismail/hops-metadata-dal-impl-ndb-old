package se.sics.hop.metadata.ndb.dalimpl.yarn.rmstatestore;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;

import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.rmstatestore.HopApplicationAttemptState;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsPredicate;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.rmstatestore.ApplicationAttemptStateDataAccess;
import se.sics.hop.metadata.yarn.tabledef.rmstatestore.ApplicationAttemptStateTableDef;
import se.sics.hop.util.CompressionUtils;

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
        HopsSession session = connector.obtainSession();
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
            HopsSession session = connector.obtainSession();
            HopsQueryBuilder qb = session.getQueryBuilder();
            HopsQueryDomainType<ApplicationAttemptStateDTO> dobj = qb.createQueryDefinition(ApplicationAttemptStateDTO.class);
            HopsPredicate pred1 = dobj.get("applicationid").equal(dobj.param("applicationid"));
            dobj.where(pred1);
            HopsQuery<ApplicationAttemptStateDTO> query = session.createQuery(dobj);
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
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ApplicationAttemptStateDTO> dobj = qb.createQueryDefinition(
            ApplicationAttemptStateDTO.class);
    HopsQuery<ApplicationAttemptStateDTO> query = session.createQuery(dobj);
    List<ApplicationAttemptStateDTO> results = query.getResultList();

    return createMap(results);
  }

    @Override
    public void createApplicationAttemptStateEntry(HopApplicationAttemptState entry) throws StorageException {
        HopsSession session = connector.obtainSession();
        session.savePersistent(createPersistable(entry, session));
    }

    @Override
    public void prepare(Collection<HopApplicationAttemptState> modified, Collection<HopApplicationAttemptState> removed) throws StorageException {
        HopsSession session = connector.obtainSession();
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
          ApplicationAttemptStateDTO entry) throws StorageException {
    ByteBuffer buffer;
    if (entry.getapplicationattempttokens() != null) {
      buffer = ByteBuffer.wrap(entry.getapplicationattempttokens());
    } else {
      buffer = null;
    }
    try {
      return new HopApplicationAttemptState(entry.getapplicationid(),
              entry.getapplicationattemptid(),
              CompressionUtils.decompress(entry.getapplicationattemptstate()),
              entry.getapplicationattempthost(),
              entry.getapplicationattemptrpcport(),
              buffer,
              entry.getapplicationattempttrakingurl());
    } catch (IOException e) {
      throw new StorageException(e);
    } catch (DataFormatException e) {
      throw new StorageException(e);
    }
  }

    private List<HopApplicationAttemptState> createHopApplicationAttemptStateList(List<ApplicationAttemptStateDTO> list)
        throws StorageException {
        List<HopApplicationAttemptState> hopList = new ArrayList<HopApplicationAttemptState>();
        for (ApplicationAttemptStateDTO dto : list) {
            hopList.add(createHopApplicationAttemptState(dto));
        }
        return hopList;

    }

  private ApplicationAttemptStateDTO createPersistable(
          HopApplicationAttemptState hop, HopsSession session) throws StorageException {
    ApplicationAttemptStateClusterJ.ApplicationAttemptStateDTO applicationAttemptStateDTO
            = session.newInstance(
                    ApplicationAttemptStateClusterJ.ApplicationAttemptStateDTO.class);

    applicationAttemptStateDTO.setapplicationid(hop.getApplicationId());
    applicationAttemptStateDTO.setapplicationattemptid(hop.
            getApplicationattemptid());
    try {
      applicationAttemptStateDTO.setapplicationattemptstate(
          CompressionUtils.compress(hop.
              getApplicationattemptstate()));
    } catch (IOException e) {
      throw new StorageException(e);
    }
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
          createMap(List<ApplicationAttemptStateDTO> results)
      throws StorageException {
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
