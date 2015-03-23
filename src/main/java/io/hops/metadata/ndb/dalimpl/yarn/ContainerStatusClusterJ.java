package io.hops.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.hops.exception.StorageException;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsSession;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import io.hops.metadata.yarn.entity.HopContainerStatus;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.tabledef.ContainerStatusTableDef;

public class ContainerStatusClusterJ implements ContainerStatusTableDef, ContainerStatusDataAccess<HopContainerStatus> {

  private static final Log LOG = LogFactory.
          getLog(ContainerStatusClusterJ.class);

  @PersistenceCapable(table = TABLE_NAME)
  public interface ContainerStatusDTO {

    @PrimaryKey
    @Column(name = CONTAINERID)
    String getcontainerid();

    void setcontainerid(String containerid);

    @PrimaryKey
    @Column(name = RMNODEID)
    String getrmnodeid();

    void setrmnodeid(String rmnodeid);

    @Column(name = STATE)
    String getstate();

    void setstate(String state);

    @Column(name = DIAGNOSTICS)
    String getdiagnostics();

    void setdiagnostics(String diagnostics);

    @Column(name = EXIT_STATUS)
    int getexitstatus();

    void setexitstatus(int exitstatus);

  }
  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public HopContainerStatus findEntry(String containerId, String rmNodeId) throws
      StorageException {
    LOG.debug("HOP :: ClusterJ ContainerStatus.findById - START");
    HopsSession session = connector.obtainSession();

    ContainerStatusDTO uciDTO;
    if (session != null) {
      uciDTO = session.find(ContainerStatusDTO.class, new Object[]{containerId,rmNodeId});
      LOG.debug("HOP :: ClusterJ ContainerStatus.findById - FINISH");
      if (uciDTO != null) {
        return createHopContainerStatus(uciDTO);
      }
    }
    return null;
  }

  @Override
  public Map<String, HopContainerStatus> getAll() throws StorageException {
    LOG.debug("HOP :: ClusterJ ContainerStatus.getAll - START");
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<ContainerStatusDTO> dobj = qb.createQueryDefinition(
            ContainerStatusDTO.class);
    HopsQuery<ContainerStatusDTO> query = session.createQuery(dobj);

    List<ContainerStatusDTO> results = query.getResultList();
    LOG.debug("HOP :: ClusterJ ContainerStatus.getAll - START");
    return createMap(results);
  }

  @Override
  public void addAll(Collection<HopContainerStatus> containersStatus) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<ContainerStatusDTO> toAdd = new ArrayList<ContainerStatusDTO>();
    for (HopContainerStatus containerStatus : containersStatus) {
      toAdd.add(createPersistable(containerStatus, session));
    }
    session.savePersistentAll(toAdd);
    session.flush();
  }

  private ContainerStatusDTO createPersistable(HopContainerStatus hopCS,
          HopsSession session) throws StorageException {
    ContainerStatusDTO csDTO = session.newInstance(ContainerStatusDTO.class);
    //Set values to persist new ContainerStatus
    csDTO.setcontainerid(hopCS.getContainerid());
    csDTO.setstate(hopCS.getState());
    csDTO.setdiagnostics(hopCS.getDiagnostics());
    csDTO.setexitstatus(hopCS.getExitstatus());
    csDTO.setrmnodeid(hopCS.getRMNodeId());
    return csDTO;
  }

  private static HopContainerStatus createHopContainerStatus(ContainerStatusDTO csDTO) {
    HopContainerStatus hop = new HopContainerStatus(csDTO.getcontainerid(),
            csDTO.getstate(), csDTO.getdiagnostics(), csDTO.getexitstatus(),
            csDTO.getrmnodeid());
    return hop;
  }

  public static Map<String, HopContainerStatus> createMap(
          List<ContainerStatusDTO> results) {
    Map<String, HopContainerStatus> map
            = new HashMap<String, HopContainerStatus>();
    for (ContainerStatusDTO persistable : results) {
      HopContainerStatus hop = createHopContainerStatus(persistable);
      map.put(hop.getContainerid(), hop);
    }
    return map;
  }
}
