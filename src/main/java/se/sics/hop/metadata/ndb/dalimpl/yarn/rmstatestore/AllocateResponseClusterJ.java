
package se.sics.hop.metadata.ndb.dalimpl.yarn.rmstatestore;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DataFormatException;

import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.rmstatestore.HopAllocateResponse;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.rmstatestore.AllocateResponseDataAccess;
import se.sics.hop.metadata.yarn.tabledef.rmstatestore.AllocateResponseTableDef;
import se.sics.hop.util.CompressionUtils;

public class AllocateResponseClusterJ implements AllocateResponseTableDef, AllocateResponseDataAccess<HopAllocateResponse> {

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
  public void addAllocateResponse(HopAllocateResponse entry) throws StorageException {
    HopsSession session = connector.obtainSession();
    session.savePersistent(createPersistable(entry, session));
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

  private AllocateResponseDTO createPersistable(HopAllocateResponse hop, HopsSession session) throws StorageException {
    AllocateResponseDTO allocateResponseDTO = session.newInstance(AllocateResponseDTO.class);

    allocateResponseDTO.setapplicationattemptid(hop.getApplicationattemptid());
    try {
      allocateResponseDTO.setallocateresponse(
          CompressionUtils.compress(hop.getAllocateResponse()));
    } catch (IOException e) {
      throw new StorageException(e);
    }

    return allocateResponseDTO;
  }
  
  private List<HopAllocateResponse> createHopAllocateResponseList(List<AllocateResponseDTO> list)
      throws StorageException {
        List<HopAllocateResponse> hopList = new ArrayList<HopAllocateResponse>();
        for (AllocateResponseDTO dto : list) {
            hopList.add(createHopAllocateResponse(dto));
        }
        return hopList;
    }

  private HopAllocateResponse createHopAllocateResponse(AllocateResponseDTO allocateResponseDTO)
      throws StorageException {
    if (allocateResponseDTO != null) {
      try {
        return new HopAllocateResponse(allocateResponseDTO.getapplicationattemptid(),
            CompressionUtils
                .decompress(allocateResponseDTO.getallocateresponse()));
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
