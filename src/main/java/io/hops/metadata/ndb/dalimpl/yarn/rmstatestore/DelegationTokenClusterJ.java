package io.hops.metadata.ndb.dalimpl.yarn.rmstatestore;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DataFormatException;

import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.entity.yarn.rmstatestore.HopDelegationToken;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.dal.rmstatestore.DelegationTokenDataAccess;
import io.hops.util.CompressionUtils;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.yarn.tabledef.rmstatestore.DelegationTokenTableDef;

public class DelegationTokenClusterJ implements DelegationTokenTableDef,
    DelegationTokenDataAccess<HopDelegationToken> {

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
    public void createDelegationTokenEntry(HopDelegationToken hopDelegationToken) throws
        StorageException {
        HopsSession session = connector.obtainSession();
        session.savePersistent(createPersistable(hopDelegationToken, session));
    }

    @Override
  public List<HopDelegationToken> getAll() throws StorageException {
      HopsSession session = connector.obtainSession();
      HopsQueryBuilder qb = session.getQueryBuilder();
      HopsQueryDomainType<DelegationTokenDTO> dobj = qb.createQueryDefinition(
              DelegationTokenDTO.class);
      HopsQuery<DelegationTokenDTO> query = session.createQuery(dobj);
      List<DelegationTokenDTO> results = query.getResultList();
      return createHopDelegationTokenList(results);

  }

    @Override
  public void remove(HopDelegationToken removed) throws StorageException {
    HopsSession session = connector.obtainSession();
    session.deletePersistent(session.newInstance(
            DelegationTokenClusterJ.DelegationTokenDTO.class, removed.
            getSeqnumber()));
  }

  private HopDelegationToken createHopDelegationToken(
          DelegationTokenDTO delegationTokenDTO)
          throws StorageException {
    try {
      return new HopDelegationToken(delegationTokenDTO.getseqnumber(),
              CompressionUtils
                  .decompress(delegationTokenDTO.getrmdtidentifier()));
    } catch (IOException e) {
      throw new StorageException(e);
    } catch (DataFormatException e) {
      throw new StorageException(e);
    }
  }

    private List<HopDelegationToken> createHopDelegationTokenList(List<DelegationTokenClusterJ.DelegationTokenDTO> list) throws StorageException {
        List<HopDelegationToken> hopList = new ArrayList<HopDelegationToken>();
        for (DelegationTokenClusterJ.DelegationTokenDTO dto : list) {
            hopList.add(createHopDelegationToken(dto));
        }
        return hopList;
    }

  private DelegationTokenDTO createPersistable(HopDelegationToken hop,
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
