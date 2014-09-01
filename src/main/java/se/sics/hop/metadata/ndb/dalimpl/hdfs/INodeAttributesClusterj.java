package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import se.sics.hop.metadata.hdfs.dal.INodeAttributesDataAccess;
import se.sics.hop.metadata.hdfs.entity.hdfs.HopINodeAttributes;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.hdfs.HopINodeCandidatePK;
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
    int getId();
    void setId(int id);

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
  public HopINodeAttributes findAttributesByPk(Integer inodeId) throws StorageException {
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
  public Collection<HopINodeAttributes> findAttributesByPkList(List<HopINodeCandidatePK> inodePks) throws StorageException {
    Session session = connector.obtainSession();
    try {
        List<HopINodeAttributes> inodeAttributesBatchResponse = new ArrayList<HopINodeAttributes>();
        List<INodeAttributesDTO> inodeAttributesBatchRequest = new ArrayList<INodeAttributesDTO>();
        for(HopINodeCandidatePK pk : inodePks){
          INodeAttributesDTO dto = session.newInstance(INodeAttributesDTO.class);
          dto.setId(pk.getInodeId());
          inodeAttributesBatchRequest.add(dto);
          session.load(dto);
        }
        
        session.flush();
        
        for(int i = 0; i < inodeAttributesBatchRequest.size();i++){
          inodeAttributesBatchResponse.add(makeINodeAttributes(inodeAttributesBatchRequest.get(i)));
        }
        return inodeAttributesBatchResponse;
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void prepare(Collection<HopINodeAttributes> modified, Collection<HopINodeAttributes> removed) throws StorageException {
    Session session = connector.obtainSession();
    try {
      List<INodeAttributesDTO> changes = new ArrayList<INodeAttributesDTO>();
      List<INodeAttributesDTO> deletions = new ArrayList<INodeAttributesDTO>();
      if (removed != null) {
        for (HopINodeAttributes attr : removed) {
          INodeAttributesDTO persistable = session.newInstance(INodeAttributesDTO.class, attr.getInodeId());
          deletions.add(persistable);
        }
      }
      if (modified != null) {
        for (HopINodeAttributes attr : modified) {
          INodeAttributesDTO persistable = createPersistable(attr, session);
          changes.add(persistable);
        }
      }
      session.deletePersistentAll(deletions);
      session.savePersistentAll(changes);
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