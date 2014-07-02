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
import se.sics.hop.metadata.hdfs.entity.yarn.HopVersion;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.VersionDataAccess;
import se.sics.hop.metadata.yarn.tabledef.VersionTableDef;

/**
 *
 * @author nickstanogias
 */
public class VersionClusterJ implements VersionTableDef, VersionDataAccess<HopVersion>{


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
    public HopVersion findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        VersionClusterJ.VersionDTO versionDTO = null;
        if (session != null) {
            versionDTO = session.find(VersionClusterJ.VersionDTO.class, id);
        }
        if (versionDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopVersion(versionDTO);
    }
    
    @Override
    public void prepare(Collection<HopVersion> modified, Collection<HopVersion> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopVersion hop : removed) {
                    VersionClusterJ.VersionDTO persistable = session.newInstance(VersionClusterJ.VersionDTO.class, hop.getId());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopVersion hop : modified) {
                    VersionClusterJ.VersionDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    private HopVersion createHopVersion(VersionDTO versionDTO) {
        return new HopVersion(versionDTO.getid(), versionDTO.getversion());
    }

    private VersionDTO createPersistable(HopVersion hop, Session session) {
        VersionClusterJ.VersionDTO versionDTO = session.newInstance(VersionClusterJ.VersionDTO.class);   
        versionDTO.setid(hop.getId());
        versionDTO.setversion(hop.getVersion());
        
        return versionDTO;
    }   
}
