package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;
import se.sics.hop.metadata.hdfs.dal.INodeAttributesDataAccess;
import se.sics.hop.metadata.hdfs.entity.hdfs.HopINodeAttributes;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.hdfs.tabledef.INodeAttributesTableDef;

/**
 *
 * @author salman
 */
public class INodeAttributesClusterj implements INodeAttributesTableDef, INodeAttributesDataAccess<HopINodeAttributes> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface INodeAttributesDTO {

    @PrimaryKey
    @Column(name = ID)
    long getId();

    void setId(long id);

    @Column(name = NSQUOTA)
    long getNSQuota();

    void setNSQuota(long nsquota);

    @Column(name = DSQUOTA)
    long getDSQuota();

    void setDSQuota(long dsquota);

    @Column(name = NSCOUNT)
    long getNSCount();

    void setNSCount(long nscount);

    @Column(name = DISKSPACE)
    long getDiskspace();

    void setDiskspace(long diskspace);
  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public HopINodeAttributes findAttributesByPk(long inodeId) throws StorageException {
    Session session = connector.obtainSession();
    try {
      INodeAttributesDTO dto = session.find(INodeAttributesDTO.class, inodeId);
      HopINodeAttributes iNodeAttributes = makeINodeAttributes(dto);
      return iNodeAttributes;
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void prepare(Collection<HopINodeAttributes> modified, Collection<HopINodeAttributes> removed) throws StorageException {
    Session session = connector.obtainSession();
    try {
      if (removed != null) {
        for (HopINodeAttributes attr : removed) {
          INodeAttributesDTO persistable = session.newInstance(INodeAttributesDTO.class, attr.getInodeId());
          session.deletePersistent(persistable);
        }
      }
      if (modified != null) {
        for (HopINodeAttributes attr : modified) {
          INodeAttributesDTO persistable = createPersistable(attr, session);
          session.savePersistent(persistable);
        }
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private INodeAttributesDTO createPersistable(HopINodeAttributes attribute, Session session) {
    INodeAttributesDTO dto = session.newInstance(INodeAttributesDTO.class);
    dto.setId(attribute.getInodeId());
    dto.setNSQuota(attribute.getNsQuota());
    dto.setNSCount(attribute.getNsCount());
    dto.setDSQuota(attribute.getDsQuota());
    dto.setDiskspace(attribute.getDiskspace());
    return dto;
  }

  private HopINodeAttributes makeINodeAttributes(INodeAttributesDTO dto) {
    if (dto == null) {
      return null;
    }
    HopINodeAttributes iNodeAttributes = new HopINodeAttributes(
            dto.getId(),
            dto.getNSQuota(),
            dto.getNSCount(),
            dto.getDSQuota(),
            dto.getDiskspace());
    return iNodeAttributes;

  }
}