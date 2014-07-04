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
import se.sics.hop.metadata.hdfs.entity.yarn.HopRMStateVersion;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.RMStateVersionDataAccess;
import se.sics.hop.metadata.yarn.tabledef.VersionTableDef;

/**
 *
 * @author nickstanogias
 */
public class RMStateVersionClusterJ implements VersionTableDef, RMStateVersionDataAccess<HopRMStateVersion>{


    @PersistenceCapable(table = TABLE_NAME)
    public interface VersionDTO {

        @PrimaryKey
        @Column(name = ID)
        int getid();
        void setid(int id);

        @Column(name = VERSION)
        byte[] getversion();
        void setversion(byte[] version);
        
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public HopRMStateVersion findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        RMStateVersionClusterJ.VersionDTO versionDTO = null;
        if (session != null) {
            versionDTO = session.find(RMStateVersionClusterJ.VersionDTO.class, id);
        }
        if (versionDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopVersion(versionDTO);
    }
    
    @Override
    public void prepare(Collection<HopRMStateVersion> modified, Collection<HopRMStateVersion> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopRMStateVersion hop : removed) {
                    RMStateVersionClusterJ.VersionDTO persistable = session.newInstance(RMStateVersionClusterJ.VersionDTO.class, hop.getId());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopRMStateVersion hop : modified) {
                    RMStateVersionClusterJ.VersionDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    private HopRMStateVersion createHopVersion(VersionDTO versionDTO) {
        return new HopRMStateVersion(versionDTO.getid(), versionDTO.getversion());
    }

    private VersionDTO createPersistable(HopRMStateVersion hop, Session session) {
        RMStateVersionClusterJ.VersionDTO versionDTO = session.newInstance(RMStateVersionClusterJ.VersionDTO.class);   
        versionDTO.setid(hop.getId());
        versionDTO.setversion(hop.getVersion());
        
        return versionDTO;
    }   
}
