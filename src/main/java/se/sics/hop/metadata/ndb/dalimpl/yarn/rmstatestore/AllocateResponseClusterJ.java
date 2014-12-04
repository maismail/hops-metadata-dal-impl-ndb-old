/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.hop.metadata.ndb.dalimpl.yarn.rmstatestore;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.List;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.rmstatestore.HopAllocateResponse;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.rmstatestore.AllocateResponseDataAccess;
import se.sics.hop.metadata.yarn.tabledef.rmstatestore.AllocateResponseTableDef;

/**
 *
 * @author gautier
 */
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
    Session session = connector.obtainSession();
    session.savePersistent(createPersistable(entry, session));
  }

  @Override
  public List<HopAllocateResponse> getAll() throws StorageException {
     Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<AllocateResponseDTO> dobj = qb.createQueryDefinition(AllocateResponseDTO.class);
      Query<AllocateResponseDTO> query = session.createQuery(dobj);
      List<AllocateResponseDTO> results = query.getResultList();
      return createHopAllocateResponseList(results);
  }

  private AllocateResponseDTO createPersistable(HopAllocateResponse hop, Session session) {
    AllocateResponseDTO allocateResponseDTO = session.newInstance(AllocateResponseDTO.class);

    allocateResponseDTO.setapplicationattemptid(hop.getApplicationattemptid());
    allocateResponseDTO.setallocateresponse(hop.getAllocateResponse());

    return allocateResponseDTO;
  }
  
  private List<HopAllocateResponse> createHopAllocateResponseList(List<AllocateResponseDTO> list) {
        List<HopAllocateResponse> hopList = new ArrayList<HopAllocateResponse>();
        for (AllocateResponseDTO dto : list) {
            hopList.add(createHopAllocateResponse(dto));
        }
        return hopList;
    }

  private HopAllocateResponse createHopAllocateResponse(AllocateResponseDTO allocateResponseDTO) {
    if (allocateResponseDTO != null) {
      return new HopAllocateResponse(allocateResponseDTO.getapplicationattemptid(),
              allocateResponseDTO.getallocateresponse());
    } else {
      return null;
    }
  }
}
