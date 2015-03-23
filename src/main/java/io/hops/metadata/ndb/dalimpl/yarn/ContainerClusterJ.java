package io.hops.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;

import io.hops.exception.StorageException;
import io.hops.metadata.yarn.entity.Container;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.dal.ContainerDataAccess;
import io.hops.metadata.yarn.tabledef.ContainerTableDef;
import io.hops.util.CompressionUtils;

public class ContainerClusterJ implements ContainerTableDef,
    ContainerDataAccess<Container> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface ContainerDTO {

    @PrimaryKey
    @Column(name = CONTAINERID_ID)
    String getcontainerid();

    void setcontainerid(String containerid);

    @Column(name = CONTAINERSTATE)
    byte[] getcontainerstate();

    void setcontainerstate(byte[] containerstate);
  }
  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public Map<String, Container> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ContainerDTO> dobj
            = qb.createQueryDefinition(
                    ContainerDTO.class);
    HopsQuery<ContainerDTO> query = session.
            createQuery(dobj);
    List<ContainerDTO> results = query.
            getResultList();
    return createMap(results);
  }

  @Override
  public void addAll(Collection<Container> toAdd) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ContainerDTO> toPersist = new ArrayList<ContainerDTO>();
    for (Container container : toAdd) {
      ContainerDTO persistable = createPersistable(container, session);
      toPersist.add(persistable);
    }
    session.savePersistentAll(toPersist);

    session.flush();
  }

  @Override
  public void createContainer(Container container) throws StorageException {
    HopsSession session = connector.obtainSession();
    session.savePersistent(createPersistable(container, session));
  }

  private Container createHopContainer(ContainerDTO containerDTO)
          throws StorageException {
    Container hop = null;
    try {
      hop = new Container(containerDTO.getcontainerid(),
              CompressionUtils.decompress(containerDTO.getcontainerstate()));
    } catch (IOException e) {
      throw new StorageException(e);
    } catch (DataFormatException e) {
      throw new StorageException(e);
    }
    return hop;
  }

  private ContainerDTO createPersistable(Container hopContainer,
          HopsSession session) throws StorageException {
    ContainerDTO containerDTO = session.newInstance(ContainerDTO.class);
    containerDTO.setcontainerid(hopContainer.getContainerId());
    try {
      containerDTO.setcontainerstate(CompressionUtils.compress(hopContainer.
              getContainerState()));
    } catch (IOException e) {
      throw new StorageException(e);
    }

    return containerDTO;
  }

  private Map<String, Container> createMap(
          List<ContainerDTO> results) throws StorageException {

    Map<String, Container> map
            = new HashMap<String, Container>();
    for (ContainerDTO dto : results) {
      Container hop
              = createHopContainer(dto);
      map.put(hop.getContainerId(), hop);
    }
    return map;
  }
}
