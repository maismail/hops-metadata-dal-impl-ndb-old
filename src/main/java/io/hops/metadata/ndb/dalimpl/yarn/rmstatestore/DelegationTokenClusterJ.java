package io.hops.metadata.ndb.dalimpl.yarn.rmstatestore;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.rmstatestore.DelegationTokenDataAccess;
import io.hops.metadata.yarn.entity.rmstatestore.DelegationToken;
import io.hops.util.CompressionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DataFormatException;

public class DelegationTokenClusterJ implements
    TablesDef.DelegationTokenTableDef,
    DelegationTokenDataAccess<DelegationToken> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface DelegationTokenDTO {

    @PrimaryKey
    @Column(name = SEQ_NUMBER)
    int getseqnumber();

    void setseqnumber(int seqnumber);

    @Column(name = RMDT_IDENTIFIER)
    byte[] getrmdtidentifier();

    void setrmdtidentifier(byte[] rmdtidentifier);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public void createDelegationTokenEntry(DelegationToken hopDelegationToken)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    session.savePersistent(createPersistable(hopDelegationToken, session));
  }

  @Override
  public List<DelegationToken> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<DelegationTokenDTO> dobj =
        qb.createQueryDefinition(DelegationTokenDTO.class);
    HopsQuery<DelegationTokenDTO> query = session.createQuery(dobj);
    List<DelegationTokenDTO> results = query.getResultList();
    return createHopDelegationTokenList(results);

  }

  @Override
  public void remove(DelegationToken removed) throws StorageException {
    HopsSession session = connector.obtainSession();
    session.deletePersistent(session
        .newInstance(DelegationTokenClusterJ.DelegationTokenDTO.class, removed.
                getSeqnumber()));
  }

  private DelegationToken createHopDelegationToken(
      DelegationTokenDTO delegationTokenDTO) throws StorageException {
    try {
      return new DelegationToken(delegationTokenDTO.getseqnumber(),
          CompressionUtils.decompress(delegationTokenDTO.getrmdtidentifier()));
    } catch (IOException e) {
      throw new StorageException(e);
    } catch (DataFormatException e) {
      throw new StorageException(e);
    }
  }

  private List<DelegationToken> createHopDelegationTokenList(
      List<DelegationTokenClusterJ.DelegationTokenDTO> list)
      throws StorageException {
    List<DelegationToken> hopList = new ArrayList<DelegationToken>();
    for (DelegationTokenClusterJ.DelegationTokenDTO dto : list) {
      hopList.add(createHopDelegationToken(dto));
    }
    return hopList;
  }

  private DelegationTokenDTO createPersistable(DelegationToken hop,
      HopsSession session) throws StorageException {
    DelegationTokenClusterJ.DelegationTokenDTO delegationTokenDTO = session.
        newInstance(DelegationTokenClusterJ.DelegationTokenDTO.class);
    delegationTokenDTO.setseqnumber(hop.getSeqnumber());
    try {
      delegationTokenDTO.setrmdtidentifier(CompressionUtils.compress(hop.
          getRmdtidentifier()));
    } catch (IOException e) {
      throw new StorageException(e);
    }

    return delegationTokenDTO;
  }
}
