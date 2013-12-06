package se.sics.hop.metadata.persistence.ndb.dalimpl;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import se.sics.hop.metadata.persistence.dal.StorageInfoDataAccess;
import se.sics.hop.metadata.persistence.entity.hdfs.HopStorageInfo;
import se.sics.hop.metadata.persistence.exceptions.StorageException;
import se.sics.hop.metadata.persistence.ndb.ClusterjConnector;
import se.sics.hop.metadata.persistence.tabledef.StorageInfoTableDef;

/**
 *
 * @author hooman
 */
public class StorageInfoClusterj implements StorageInfoTableDef, StorageInfoDataAccess<HopStorageInfo> {

  private HopStorageInfo createStorageInfo(StorageInfoDTO dto) {
    return new HopStorageInfo(
            dto.getId(),
            dto.getLayoutVersion(),
            dto.getNamespaceId(),
            dto.getClusterId(),
            dto.getCreationTime(),
            dto.getBlockPoolId());
  }

  @PersistenceCapable(table = TABLE_NAME)
  public interface StorageInfoDTO {

    @PrimaryKey
    @Column(name = ID)
    int getId();

    void setId(int id);

    @Column(name = LAYOUT_VERSION)
    int getLayoutVersion();

    void setLayoutVersion(int layoutVersion);

    @Column(name = NAMESPACE_ID)
    int getNamespaceId();

    void setNamespaceId(int namespaceId);

    @Column(name = CLUSTER_ID)
    String getClusterId();

    void setClusterId(String clusterId);

    @Column(name = CREATION_TIME)
    long getCreationTime();

    void setCreationTime(long creationTime);

    @Column(name = BLOCK_POOL_ID)
    String getBlockPoolId();

    void setBlockPoolId(String bpid);
  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public HopStorageInfo findByPk(int infoType) throws StorageException {
    try {
      Session session = connector.obtainSession();
      StorageInfoDTO si = session.find(StorageInfoDTO.class, infoType);
      if (si == null) {
        return null;
      }
      return createStorageInfo(si);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void prepare(HopStorageInfo storageInfo) throws StorageException {
    try {
      Session session = connector.obtainSession();
      StorageInfoDTO dto = session.newInstance(StorageInfoDTO.class);
      dto.setId(storageInfo.getId());
      dto.setClusterId(storageInfo.getClusterId());
      dto.setLayoutVersion(storageInfo.getLayoutVersion());
      dto.setNamespaceId(storageInfo.getNamespaceId());
      dto.setCreationTime(storageInfo.getCreationTime());
      dto.setBlockPoolId(storageInfo.getBlockPoolId());
      session.savePersistent(dto);
    } catch (Exception ex) {
      throw new StorageException(ex);
    }
  }
}
