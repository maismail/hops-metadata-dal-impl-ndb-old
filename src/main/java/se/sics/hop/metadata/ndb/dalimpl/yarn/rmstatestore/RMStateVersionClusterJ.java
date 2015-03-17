package se.sics.hop.metadata.ndb.dalimpl.yarn.rmstatestore;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.rmstatestore.HopRMStateVersion;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.rmstatestore.RMStateVersionDataAccess;
import se.sics.hop.metadata.yarn.tabledef.rmstatestore.RMStateVersionTableDef;

public class RMStateVersionClusterJ implements RMStateVersionTableDef, RMStateVersionDataAccess<HopRMStateVersion> {

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
        HopsSession session = connector.obtainSession();

        RMStateVersionClusterJ.VersionDTO versionDTO = null;
        if (session != null) {
            versionDTO = session.find(RMStateVersionClusterJ.VersionDTO.class, id);
        }

        return createHopVersion(versionDTO);
    }

    @Override
    public void prepare(Collection<HopRMStateVersion> modified, Collection<HopRMStateVersion> removed) throws StorageException {
        HopsSession session = connector.obtainSession();
        try {
            if (removed != null) {
                List<VersionDTO> toRemove = new ArrayList<VersionDTO>();
                for (HopRMStateVersion hop : removed) {
                    toRemove.add(session.newInstance(RMStateVersionClusterJ.VersionDTO.class, hop.getId()));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<VersionDTO> toModify = new ArrayList<VersionDTO>();
                for (HopRMStateVersion hop : modified) {
                    toModify.add(createPersistable(hop, session));
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    private HopRMStateVersion createHopVersion(VersionDTO versionDTO) {
      if(versionDTO!=null){
        return new HopRMStateVersion(versionDTO.getid(), versionDTO.getversion());
      }else{
        return null;
      }
    }

    private VersionDTO createPersistable(HopRMStateVersion hop, HopsSession session) throws StorageException {
        RMStateVersionClusterJ.VersionDTO versionDTO = session.newInstance(RMStateVersionClusterJ.VersionDTO.class);
        versionDTO.setid(hop.getId());
        versionDTO.setversion(hop.getVersion());

        return versionDTO;
    }
}
