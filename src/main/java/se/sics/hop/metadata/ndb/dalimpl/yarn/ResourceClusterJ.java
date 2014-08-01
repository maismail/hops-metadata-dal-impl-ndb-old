package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopResource;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.ResourceDataAccess;
import se.sics.hop.metadata.yarn.tabledef.ResourceTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class ResourceClusterJ implements ResourceTableDef, ResourceDataAccess<HopResource> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface ResourceDTO {

        @PrimaryKey
        @Column(name = ID)
        String getId();

        void setId(String id);

        @Column(name = MEMORY)
        int getMemory();

        void setMemory(int memory);

        @Column(name = VIRTUAL_CORES)
        int getVirtualcores();

        void setVirtualcores(int virtualcores);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopResource findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        ResourceDTO resourceDTO = null;
        if (session != null) {
            resourceDTO = session.find(ResourceDTO.class, id);
        }
        if (resourceDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row:" + id);
        }

        return createHopResource(resourceDTO);
    }

    @Override
    public void prepare(Collection<HopResource> modified, Collection<HopResource> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                List<ResourceDTO> toRemove = new ArrayList<ResourceDTO>();
                for (HopResource req : removed) {
                    toRemove.add(session.newInstance(ResourceDTO.class, req.getId()));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<ResourceDTO> toModify = new ArrayList<ResourceDTO>();
                for (HopResource req : modified) {
                    toModify.add(createPersistable(req, session));
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException("Error while modifying resources, error:" + e.getMessage());
        }
    }

    @Override
    public void createResource(HopResource resourceNode) throws StorageException {
        Session session = connector.obtainSession();
        session.savePersistent(createPersistable(resourceNode, session));
    }

    private HopResource createHopResource(ResourceDTO resourceDTO) {
        return new HopResource(resourceDTO.getId(), resourceDTO.getMemory(), resourceDTO.getVirtualcores());
    }

    private ResourceDTO createPersistable(HopResource resource, Session session) {
        ResourceDTO resourceDTO = session.newInstance(ResourceDTO.class);
        resourceDTO.setId(resource.getId());
        resourceDTO.setMemory(resource.getMemory());
        resourceDTO.setVirtualcores(resource.getVirtualcores());
        return resourceDTO;
    }
}
