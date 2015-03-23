package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopRMContainer;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.RMContainerDataAccess;
import se.sics.hop.metadata.yarn.tabledef.RMContainerTableDef;

public class RMContainerClusterJ implements RMContainerTableDef, RMContainerDataAccess<HopRMContainer> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface RMContainerDTO {

    @PrimaryKey
    @Column(name = CONTAINERID_ID)
    String getcontaineridid();

    void setcontaineridid(String containeridid);

    @Column(name = APPLICATIONATTEMPTID_ID)
    String getappattemptidid();

    void setappattemptidid(String appattemptidid);

    @Column(name = NODEID_ID)
    String getnodeidid();

    void setnodeidid(String nodeidid);

    @Column(name = USER)
    String getuser();

    void setuser(String user);

    @Column(name = STARTTIME)
    long getstarttime();

    void setstarttime(long starttime);

    @Column(name = FINISHTIME)
    long getfinishtime();

    void setfinishtime(long finishtime);

    @Column(name = STATE)
    String getstate();

    void setstate(String state);

    @Column(name = FINISHEDSTATUSSTATE)
    String getfinishedstatusstate();

    void setfinishedstatusstate(String finishedstatusstate);

    @Column(name = EXITSTATUS)
    int getexitstatus();

    void setexitstatus(int exitstatus);

  }
  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public Map<String, HopRMContainer> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<RMContainerDTO> dobj
            = qb.createQueryDefinition(
                    RMContainerDTO.class);
    HopsQuery<RMContainerDTO> query = session.
            createQuery(dobj);
    List<RMContainerDTO> results = query.
            getResultList();
    return createMap(results);
  }

  @Override

  public void addAll(Collection<HopRMContainer> toAdd) throws StorageException {
    HopsSession session = connector.obtainSession();

    List<RMContainerDTO> toPersist = new ArrayList<RMContainerDTO>(toAdd.size());
    for (HopRMContainer hop : toAdd) {
      toPersist.add(createPersistable(hop, session));
    }

    session.savePersistentAll(toPersist);
  }

  @Override
  public void removeAll(Collection<HopRMContainer> toRemove) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<RMContainerDTO> toPersist = new ArrayList<RMContainerDTO>(toRemove.
            size());
    for (HopRMContainer hop : toRemove) {
      toPersist.add(session.
              newInstance(RMContainerClusterJ.RMContainerDTO.class, hop.
                      getContainerIdID()));
    }
    session.deletePersistentAll(toPersist);
  }

  @Override
  public void add(HopRMContainer rmcontainer) throws StorageException {
    HopsSession session = connector.obtainSession();
    session.savePersistent(createPersistable(rmcontainer, session));
  }

  private HopRMContainer createHopRMContainer(RMContainerDTO rMContainerDTO) {

    return new HopRMContainer(
            rMContainerDTO.getcontaineridid(),
            rMContainerDTO.getappattemptidid(),
            rMContainerDTO.getnodeidid(),
            rMContainerDTO.getuser(),
            rMContainerDTO.getstarttime(),
            rMContainerDTO.getfinishtime(),
            rMContainerDTO.getstate(),
            rMContainerDTO.getfinishedstatusstate(),
            rMContainerDTO.getexitstatus());

  }

  private RMContainerDTO createPersistable(HopRMContainer hop,
          HopsSession session) throws StorageException {
    RMContainerClusterJ.RMContainerDTO rMContainerDTO = session.newInstance(
            RMContainerClusterJ.RMContainerDTO.class);

    rMContainerDTO.setcontaineridid(hop.getContainerIdID());
    rMContainerDTO.setappattemptidid(hop.getApplicationAttemptIdID());
    rMContainerDTO.setnodeidid(hop.getNodeIdID());
    rMContainerDTO.setuser(hop.getUser());
    rMContainerDTO.setstarttime(hop.getStarttime());
    rMContainerDTO.setfinishtime(hop.getFinishtime());
    rMContainerDTO.setstate(hop.getState());
    rMContainerDTO.setfinishedstatusstate(hop.getFinishedStatusState());
    rMContainerDTO.setexitstatus(hop.getExitStatus());

    return rMContainerDTO;
  }

  private Map<String, HopRMContainer> createMap(
          List<RMContainerDTO> results) {
    Map<String, HopRMContainer> map
            = new HashMap<String, HopRMContainer>();
    for (RMContainerDTO dto : results) {
      HopRMContainer hop
              = createHopRMContainer(dto);
      map.put(hop.getContainerIdID(), hop);
    }
    return map;
  }
}
