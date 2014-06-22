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
import se.sics.hop.metadata.hdfs.entity.yarn.HopAllocateResponse;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.AllocateResponseDataAccess;
import se.sics.hop.metadata.yarn.tabledef.AllocateResponseTableDef;


/**
 *
 * @author nickstanogias
 */
public class AllocateResponseClusterJ implements AllocateResponseTableDef, AllocateResponseDataAccess<HopAllocateResponse>{


    @PersistenceCapable(table = TABLE_NAME)
    public interface AllocateResponseDTO {

        @PrimaryKey
        @Column(name = ID)
        int getid();
        void setid(int id);

        @Column(name = LIMIT)
        int getlimit();
        void setlimit(int limit);
        
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public HopAllocateResponse findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        AllocateResponseClusterJ.AllocateResponseDTO allocateResponseDTO = null;
        if (session != null) {
            allocateResponseDTO = session.find(AllocateResponseClusterJ.AllocateResponseDTO.class, id);
        }
        if (allocateResponseDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopAllocateResponse(allocateResponseDTO);
    }

    @Override
    public void prepare(Collection<HopAllocateResponse> modified, Collection<HopAllocateResponse> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopAllocateResponse hop : removed) {
                    AllocateResponseClusterJ.AllocateResponseDTO persistable = session.newInstance(AllocateResponseClusterJ.AllocateResponseDTO.class, hop.getId());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopAllocateResponse hop : modified) {
                    AllocateResponseClusterJ.AllocateResponseDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    private HopAllocateResponse createHopAllocateResponse(AllocateResponseDTO allocateResponseDTO) {
        return new HopAllocateResponse(allocateResponseDTO.getid(),
                                        allocateResponseDTO.getlimit());
    }

    private AllocateResponseDTO createPersistable(HopAllocateResponse hop, Session session) {
        AllocateResponseClusterJ.AllocateResponseDTO allocateResponseDTO = session.newInstance(AllocateResponseClusterJ.AllocateResponseDTO.class);
        
        allocateResponseDTO.setid(hop.getId());
        allocateResponseDTO.setlimit(hop.getLimit());
        
        return allocateResponseDTO;
    }
    
}
