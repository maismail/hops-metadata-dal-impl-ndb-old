package io.hops.metadata.ndb.dalimpl.yarn.rmstatestore;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.zip.DataFormatException;

import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import io.hops.metadata.yarn.tabledef.rmstatestore.ApplicationStateTableDef;
import io.hops.util.CompressionUtils;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationState;

public class ApplicationStateClusterJ implements ApplicationStateTableDef,
    ApplicationStateDataAccess<ApplicationState> {

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
    public ApplicationState findByApplicationId(String id) throws
        StorageException {
        HopsSession session = connector.obtainSession();

        ApplicationStateDTO appStateDTO = null;
        if (session != null) {
            appStateDTO = session.find(ApplicationStateDTO.class, id);
        }
       
        return createHopApplicationState(appStateDTO);
    }

    @Override
    public List<ApplicationState> getAll() throws StorageException {
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
  public void addAll(Collection<ApplicationState> toAdd) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<ApplicationStateDTO> toPersist = new ArrayList<ApplicationStateDTO>();
    for (ApplicationState req : toAdd) {
      toPersist.add(createPersistable(req, session));
    }
    session.savePersistentAll(toPersist);
  }

  @Override
  public void removeAll(Collection<ApplicationState> toRemove) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<ApplicationStateDTO> toPersist = new ArrayList<ApplicationStateDTO>();
    for (ApplicationState entry : toRemove) {
      toPersist.add(session.newInstance(ApplicationStateDTO.class, entry.
              getApplicationId()));
    }
    session.deletePersistentAll(toPersist);
  }

  @Override
  public void add(ApplicationState toAdd) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    session.savePersistent(createPersistable(toAdd, session));
  }

  @Override
  public void remove(ApplicationState toRemove) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    session.deletePersistent(session.newInstance(ApplicationStateDTO.class,
            toRemove.
            getApplicationId()));
  }
  
  private ApplicationState createHopApplicationState(
          ApplicationStateDTO appStateDTO) throws StorageException {
    if (appStateDTO != null) {
      ApplicationState state = null;
      try {
        state = new ApplicationState(appStateDTO.getapplicationid(),
                CompressionUtils.decompress(appStateDTO.getappstate()), appStateDTO.
                getappuser(),
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

    private List<ApplicationState> createHopApplicationStateList(List<ApplicationStateDTO> list) throws StorageException {
        List<ApplicationState> hopList = new ArrayList<ApplicationState>();
        for (ApplicationStateDTO dto : list) {
            hopList.add(createHopApplicationState(dto));
        }
        return hopList;

    }

  private ApplicationStateDTO createPersistable(ApplicationState hop,
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
