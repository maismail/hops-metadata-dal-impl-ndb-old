package se.sics.hop.metadata.ndb.dalimpl.yarn;

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
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopContainer;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.ContainerDataAccess;
import se.sics.hop.metadata.yarn.tabledef.ContainerTableDef;
import se.sics.hop.util.CompressionUtils;

public class ContainerClusterJ implements ContainerTableDef, ContainerDataAccess<HopContainer> {

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
  public Map<String, HopContainer> getAll() throws StorageException {
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
  public void addAll(Collection<HopContainer> toAdd) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ContainerDTO> toPersist = new ArrayList<ContainerDTO>();
    for (HopContainer container : toAdd) {
      ContainerDTO persistable = createPersistable(container, session);
      toPersist.add(persistable);
    }
    session.savePersistentAll(toPersist);

    session.flush();
  }

  @Override
  public void createContainer(HopContainer container) throws StorageException {
    HopsSession session = connector.obtainSession();
    session.savePersistent(createPersistable(container, session));
  }

  private HopContainer createHopContainer(ContainerDTO containerDTO)
          throws StorageException {
    HopContainer hop = null;
    try {
      hop = new HopContainer(containerDTO.getcontainerid(),
              CompressionUtils.decompress(containerDTO.getcontainerstate()));
    } catch (IOException e) {
      throw new StorageException(e);
    } catch (DataFormatException e) {
      throw new StorageException(e);
    }
    return hop;
  }

  private ContainerDTO createPersistable(HopContainer hopContainer,
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

  private Map<String, HopContainer> createMap(
          List<ContainerDTO> results) throws StorageException {

    Map<String, HopContainer> map
            = new HashMap<String, HopContainer>();
    for (ContainerDTO dto : results) {
      HopContainer hop
              = createHopContainer(dto);
      map.put(hop.getContainerId(), hop);
    }
    return map;
  }
}
