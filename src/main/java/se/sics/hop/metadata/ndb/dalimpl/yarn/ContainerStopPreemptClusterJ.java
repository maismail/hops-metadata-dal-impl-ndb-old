/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopContainerStopPreemt;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.ContainerStopPreemtDataAccess;
import se.sics.hop.metadata.yarn.tabledef.ContainerStopPreemptTableDef;

/**
 *
 * @author nickstanogias
 */
public class ContainerStopPreemptClusterJ implements ContainerStopPreemptTableDef, ContainerStopPreemtDataAccess<HopContainerStopPreemt>{
    
    @PersistenceCapable(table = TABLE_NAME)
    public interface ContainerStopPreemptDTO {

        @PrimaryKey
        @Column(name = FICASCHEDULERAPP_ID)
        int getficaschedulerappid();
        void setficaschedulerappid(int ficaschedulerappid);

        @Column(name = CONTAINER_ID)
        int getcontainerid();
        void setcontainerid(int containerid);
        
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public HopContainerStopPreemt findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        ContainerStopPreemptClusterJ.ContainerStopPreemptDTO containerStopPreemptDTO = null;
        if (session != null) {
            containerStopPreemptDTO = session.find(ContainerStopPreemptClusterJ.ContainerStopPreemptDTO.class, id);
        }
        if (containerStopPreemptDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopContainerStopPreemt(containerStopPreemptDTO);
    }

    @Override
    public void prepare(Collection<HopContainerStopPreemt> modified, Collection<HopContainerStopPreemt> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopContainerStopPreemt hop : removed) {
                    ContainerStopPreemptClusterJ.ContainerStopPreemptDTO persistable = session.newInstance(ContainerStopPreemptClusterJ.ContainerStopPreemptDTO.class, hop.getFicaschedulerapp_id());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopContainerStopPreemt hop : modified) {
                    ContainerStopPreemptClusterJ.ContainerStopPreemptDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    private HopContainerStopPreemt createHopContainerStopPreemt(ContainerStopPreemptDTO containerStopPreemptDTO) {
        return new HopContainerStopPreemt(containerStopPreemptDTO.getficaschedulerappid(),
                                        containerStopPreemptDTO.getcontainerid());
    }

    private ContainerStopPreemptDTO createPersistable(HopContainerStopPreemt hop, Session session) {
        ContainerStopPreemptClusterJ.ContainerStopPreemptDTO containerStopPreemptDTO = session.newInstance(ContainerStopPreemptClusterJ.ContainerStopPreemptDTO.class);
        
        containerStopPreemptDTO.setficaschedulerappid(hop.getFicaschedulerapp_id());
        containerStopPreemptDTO.setcontainerid(hop.getContainer_id());
        
        return containerStopPreemptDTO;
    }
    
}
