
package io.hops.metadata.ndb.dalimpl.yarn.rmstatestore;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.zip.DataFormatException;

import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.dal.rmstatestore.AllocateResponseDataAccess;
import io.hops.metadata.yarn.tabledef.rmstatestore.AllocateResponseTableDef;
import io.hops.util.CompressionUtils;
import io.hops.metadata.yarn.entity.rmstatestore.HopAllocateResponse;

public class AllocateResponseClusterJ implements AllocateResponseTableDef,
    AllocateResponseDataAccess<HopAllocateResponse> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface AllocateResponseDTO {

    @PrimaryKey
    @Column(name = APPLICATIONATTEMPTID)
    String getapplicationattemptid();

    void setapplicationattemptid(String applicationattemptid);

    @Column(name = ALLOCATERESPONSE)
    byte[] getallocateresponse();

    void setallocateresponse(byte[] allocateresponse);
  }
  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public void addAll(Collection<HopAllocateResponse> toAdd) throws
      StorageException {
    HopsSession session = connector.obtainSession();
    List<AllocateResponseDTO> toPersist = new ArrayList<AllocateResponseDTO>();
    for (HopAllocateResponse req : toAdd) {
      toPersist.add(createPersistable(req, session));
    }
    session.savePersistentAll(toPersist);
  }

  @Override
  public void removeAll(Collection<HopAllocateResponse> toAdd) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<AllocateResponseDTO> toPersist = new ArrayList<AllocateResponseDTO>();
    for (HopAllocateResponse req : toAdd) {
      AllocateResponseDTO persistable = session.newInstance(AllocateResponseDTO.class,
              req.getApplicationattemptid());
      toPersist.add(persistable);
    }
    session.deletePersistentAll(toPersist);
  }
  
  @Override
  public List<HopAllocateResponse> getAll() throws StorageException {
     HopsSession session = connector.obtainSession();
      HopsQueryBuilder qb = session.getQueryBuilder();
      HopsQueryDomainType<AllocateResponseDTO> dobj = qb.createQueryDefinition(AllocateResponseDTO.class);
      HopsQuery<AllocateResponseDTO> query = session.createQuery(dobj);
      List<AllocateResponseDTO> results = query.getResultList();
      return createHopAllocateResponseList(results);
  }

  private AllocateResponseDTO createPersistable(HopAllocateResponse hop,
          HopsSession session) throws StorageException {
    AllocateResponseDTO allocateResponseDTO = session.newInstance(
            AllocateResponseDTO.class);

    allocateResponseDTO.setapplicationattemptid(hop.getApplicationattemptid());
    try {
      allocateResponseDTO.setallocateresponse(CompressionUtils.compress(hop.
          getAllocateResponse()));
    } catch (IOException e) {
      throw new StorageException(e);
    }

    return allocateResponseDTO;
  }
  
  private List<HopAllocateResponse> createHopAllocateResponseList(List<AllocateResponseDTO> list) throws StorageException {
        List<HopAllocateResponse> hopList = new ArrayList<HopAllocateResponse>();
        for (AllocateResponseDTO dto : list) {
            hopList.add(createHopAllocateResponse(dto));
        }
        return hopList;
    }

  private HopAllocateResponse createHopAllocateResponse(
          AllocateResponseDTO allocateResponseDTO) throws StorageException {
    if (allocateResponseDTO != null) {
      try {
        return new HopAllocateResponse(allocateResponseDTO.
                getapplicationattemptid(),
                CompressionUtils.
                decompress(allocateResponseDTO.getallocateresponse()));
      } catch (IOException e) {
        throw new StorageException(e);
      } catch (DataFormatException e) {
        throw new StorageException(e);
      }
    } else {
      return null;
    }
  }
}
