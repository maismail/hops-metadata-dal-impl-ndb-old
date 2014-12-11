package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopContainer;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.ContainerDataAccess;
import se.sics.hop.metadata.yarn.tabledef.ContainerTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class ContainerClusterJ implements ContainerTableDef, ContainerDataAccess<HopContainer> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface ContainerDTO {

        @PrimaryKey
        @Column(name = CONTAINERID_ID)
        String getcontaineridid();
        void setcontaineridid(String containeridid);

        @Column(name = CONTAINERSTATE)
        byte[] getcontainerstate();
        void setcontainerstate(byte[] containerstate);      
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopContainer findById(String id) throws StorageException {
        Session session = connector.obtainSession();

        ContainerDTO containerDTO = null;
        if (session != null) {
            containerDTO = session.find(ContainerDTO.class, id);
        }
        if (containerDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopContainer(containerDTO);
    }

  @Override
  public Map<String, HopContainer> getAll() throws StorageException {
    Session session = connector.obtainSession();
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<ContainerDTO> dobj
            = qb.createQueryDefinition(
                    ContainerDTO.class);
    Query<ContainerDTO> query = session.
            createQuery(dobj);
    List<ContainerDTO> results = query.
            getResultList();
    return createMap(results);
  }
    
    @Override
    public void prepare(Collection<HopContainer> modified, Collection<HopContainer> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopContainer hopContainer : removed) {

                    ContainerDTO persistable = session.newInstance(ContainerDTO.class, hopContainer.getContainerIdID());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopContainer hopContainer : modified) {
                    ContainerDTO persistable = createPersistable(hopContainer, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createContainer(HopContainer container) throws StorageException {
        Session session = connector.obtainSession();
        session.savePersistent(createPersistable(container, session));
    }

    private HopContainer createHopContainer(ContainerDTO containerDTO) {
        HopContainer hop = new HopContainer(containerDTO.getcontaineridid(),
                containerDTO.getcontainerstate());
        return hop;
    }

    private ContainerDTO createPersistable(HopContainer hopContainer, Session session) {
        ContainerDTO containerDTO = session.newInstance(ContainerDTO.class);
        containerDTO.setcontaineridid(hopContainer.getContainerIdID());
        containerDTO.setcontainerstate(hopContainer.getContainerstate());
        
        return containerDTO;
    }
    
  private Map<String,HopContainer> createMap(
          List<ContainerDTO> results) {
    Map<String, HopContainer> map
            = new HashMap<String, HopContainer>();
    for (ContainerDTO dto : results) {
      HopContainer hop
              = createHopContainer(dto);
      map.put(hop.getContainerIdID(), hop);
    }
    return map;
  }
}
