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

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
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

//    @Column(name = RESERVED_NODEID_ID)
//    String getreservednodeid();
//
//    void setreservednodeid(String reservednodeid);

//    @Column(name = RESERVED_PRIORITY_ID)
//    int getreservedpriorityid();
//
//    void setreservedpriorityid(int reservedpriorityid);

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
  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public HopRMContainer findById(String id) throws StorageException {
    HopsSession session = connector.obtainSession();

    RMContainerDTO rMContainerDTO = null;
    if (session != null) {
      rMContainerDTO = session.find(RMContainerDTO.class, id);
    }

    return createHopRMContainer(rMContainerDTO);
  }

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
  public void prepare(Collection<HopRMContainer> modified, Collection<HopRMContainer> removed) throws StorageException {
    HopsSession session = connector.obtainSession();
    try {
      if (removed != null) {
        List<RMContainerDTO> toRemove = new ArrayList<RMContainerDTO>(removed.size());
        for (HopRMContainer hop : removed) {
          toRemove.add(session.newInstance(RMContainerClusterJ.RMContainerDTO.class, hop.getContainerIdID()));
        }
        session.deletePersistentAll(toRemove);
      }
      if (modified != null) {
        List<RMContainerDTO> toModify = new ArrayList<RMContainerDTO>(modified.size());
        for (HopRMContainer hop : modified) {
          toModify.add(createPersistable(hop, session));
        }
        session.savePersistentAll(toModify);
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void createRMContainer(HopRMContainer rmcontainer) throws StorageException {
    HopsSession session = connector.obtainSession();
    session.savePersistent(createPersistable(rmcontainer, session));
  }

  private HopRMContainer createHopRMContainer(RMContainerDTO rMContainerDTO) {
  
      return new HopRMContainer(
              rMContainerDTO.getcontaineridid(),
              rMContainerDTO.getappattemptidid(),
              rMContainerDTO.getnodeidid(),
              rMContainerDTO.getuser(),
//              rMContainerDTO.getreservednodeid(),
//              rMContainerDTO.getreservedpriorityid(),
              rMContainerDTO.getstarttime(),
              rMContainerDTO.getfinishtime(),
              rMContainerDTO.getstate(), /*null, null,*/
              rMContainerDTO.getfinishedstatusstate(),
              rMContainerDTO.getexitstatus());
    
  }

  private RMContainerDTO createPersistable(HopRMContainer hop, HopsSession session) throws StorageException {
    RMContainerClusterJ.RMContainerDTO rMContainerDTO = session.newInstance(RMContainerClusterJ.RMContainerDTO.class);

    rMContainerDTO.setcontaineridid(hop.getContainerIdID());
    rMContainerDTO.setappattemptidid(hop.getApplicationAttemptIdID());
    rMContainerDTO.setnodeidid(hop.getNodeIdID());
    rMContainerDTO.setuser(hop.getUser());
//    rMContainerDTO.setreservednodeid(hop.getReservedNodeIdID());
//    rMContainerDTO.setreservedpriorityid(hop.getReservedPriorityID());
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
