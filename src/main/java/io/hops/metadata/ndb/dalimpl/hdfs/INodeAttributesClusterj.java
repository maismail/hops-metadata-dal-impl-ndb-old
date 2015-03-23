package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.dal.INodeAttributesDataAccess;
import io.hops.metadata.hdfs.entity.INodeAttributes;
import io.hops.metadata.hdfs.entity.INodeCandidatePrimaryKey;
import io.hops.metadata.hdfs.tabledef.INodeAttributesTableDef;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class INodeAttributesClusterj implements INodeAttributesTableDef, INodeAttributesDataAccess<INodeAttributes> {

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
  public INodeAttributes findAttributesByPk(Integer inodeId) throws StorageException {
    HopsSession session = connector.obtainSession();
    INodeAttributesDTO dto = session.find(INodeAttributesDTO.class, inodeId);
    INodeAttributes iNodeAttributes = makeINodeAttributes(dto);
    return iNodeAttributes;
  }
  
  @Override
  public Collection<INodeAttributes> findAttributesByPkList(List<INodeCandidatePrimaryKey> inodePks) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<INodeAttributes> inodeAttributesBatchResponse = new ArrayList<INodeAttributes>();
    List<INodeAttributesDTO> inodeAttributesBatchRequest = new ArrayList<INodeAttributesDTO>();
    for(INodeCandidatePrimaryKey pk : inodePks){
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
  }

  @Override
  public void prepare(Collection<INodeAttributes> modified, Collection<INodeAttributes> removed) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<INodeAttributesDTO> changes = new ArrayList<INodeAttributesDTO>();
    List<INodeAttributesDTO> deletions = new ArrayList<INodeAttributesDTO>();
    if (removed != null) {
      for (INodeAttributes attr : removed) {
        INodeAttributesDTO persistable = session.newInstance(
            INodeAttributesDTO.class, attr.getInodeId());
        deletions.add(persistable);
      }
    }
    if (modified != null) {
      for (INodeAttributes attr : modified) {
        INodeAttributesDTO persistable = createPersistable(attr, session);
        changes.add(persistable);
      }
    }
    session.deletePersistentAll(deletions);
    session.savePersistentAll(changes);
  }

  private INodeAttributesDTO createPersistable(INodeAttributes attribute, HopsSession session)
      throws StorageException {
    INodeAttributesDTO dto = session.newInstance(INodeAttributesDTO.class);
    dto.setId(attribute.getInodeId());
    dto.setNSQuota(attribute.getNsQuota());
    dto.setNSCount(attribute.getNsCount());
    dto.setDSQuota(attribute.getDsQuota());
    dto.setDiskspace(attribute.getDiskspace());
    return dto;
  }

  private INodeAttributes makeINodeAttributes(INodeAttributesDTO dto) {
    if (dto == null) {
      return null;
    }
    INodeAttributes iNodeAttributes = new INodeAttributes(
            dto.getId(),
            dto.getNSQuota(),
            dto.getNSCount(),
            dto.getDSQuota(),
            dto.getDiskspace());
    return iNodeAttributes;
  }
}