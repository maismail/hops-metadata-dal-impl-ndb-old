package se.sics.hop.metadata.ndb.dalimpl.yarn.rmstatestore;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.rmstatestore.HopApplicationState;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import se.sics.hop.metadata.yarn.tabledef.rmstatestore.ApplicationStateTableDef;
import se.sics.hop.util.CompressionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.zip.DataFormatException;

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
        
        @Column(name = USER)
        String getappuser();

        void setappuser(String user);
        
        @Column(name = NAME)
        String getappname();

        void setappname(String name);
        
        @Column(name = SMSTATE)
        String getappsmstate();

        void setappsmstate(String appstate);
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopApplicationState findByApplicationId(String id) throws StorageException {
        HopsSession session = connector.obtainSession();

        ApplicationStateDTO appStateDTO = null;
        if (session != null) {
            appStateDTO = session.find(ApplicationStateDTO.class, id);
        }
       
        return createHopApplicationState(appStateDTO);
    }

    @Override
    public List<HopApplicationState> getAll() throws StorageException {
        try {
            HopsSession session = connector.obtainSession();
            HopsQueryBuilder qb = session.getQueryBuilder();
            HopsQueryDomainType<ApplicationStateDTO> dobj = qb.createQueryDefinition(ApplicationStateDTO.class);
            //Predicate pred1 = dobj.get("applicationid").equal(dobj.param("applicationid"));
            //dobj.where(pred1);
            HopsQuery<ApplicationStateDTO> query = session.createQuery(dobj);
            //query.setParameter("applicationid", applicationid);
            List<ApplicationStateDTO> results = query.getResultList();
                return createHopApplicationStateList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }

    }

    @Override
    public void prepare(Collection<HopApplicationState> modified, Collection<HopApplicationState> removed) throws StorageException {
        HopsSession session = connector.obtainSession();
        try {
            if (removed != null) {
                List<ApplicationStateDTO> toRemove = new ArrayList<ApplicationStateDTO>();
                for (HopApplicationState hop : removed) {
                    toRemove.add(session.newInstance(ApplicationStateDTO.class, hop.getApplicationid()));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<ApplicationStateDTO> toModify = new ArrayList<ApplicationStateDTO>();
                for (HopApplicationState hop : modified) {
                    toModify.add(createPersistable(hop, session));
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

  private HopApplicationState createHopApplicationState(
          ApplicationStateDTO appStateDTO) throws StorageException {
    if (appStateDTO != null) {
      HopApplicationState state = null;
      try {
        state = new HopApplicationState(appStateDTO.getapplicationid(),
            CompressionUtils.decompress(appStateDTO.getappstate()), appStateDTO.getappuser(),
                appStateDTO.getappname(), appStateDTO.getappsmstate());
      } catch (IOException e) {
        throw new StorageException(e);
      } catch (DataFormatException e) {
        throw new StorageException(e);
      }
      return state;
    } else {
      return null;
    }
  }

    private List<HopApplicationState> createHopApplicationStateList(List<ApplicationStateDTO> list)
        throws StorageException {
        List<HopApplicationState> hopList = new ArrayList<HopApplicationState>();
        for (ApplicationStateDTO dto : list) {
            hopList.add(createHopApplicationState(dto));
        }
        return hopList;

    }

  private ApplicationStateDTO createPersistable(HopApplicationState hop,
          HopsSession session) throws StorageException {
    ApplicationStateDTO appStateDTO = session.newInstance(
            ApplicationStateClusterJ.ApplicationStateDTO.class);
    appStateDTO.setapplicationid(hop.getApplicationid());
    try {
      appStateDTO.setappstate(CompressionUtils.compress(hop.getAppstate()));
    } catch (IOException e) {
      throw new StorageException(e);
    }
    appStateDTO.setappuser(hop.getUser());
    appStateDTO.setappname(hop.getName());
    appStateDTO.setappsmstate(hop.getState());

    return appStateDTO;
  }
}
