package se.sics.hop.metadata.ndb.dalimpl.yarn.rmstatestore;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.zip.DataFormatException;

import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.rmstatestore.HopDelegationToken;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.rmstatestore.DelegationTokenDataAccess;
import se.sics.hop.metadata.yarn.tabledef.rmstatestore.DelegationTokenTableDef;
import se.sics.hop.util.CompressionUtils;

/**
 *
 * @author nickstanogias
 */
public class DelegationTokenClusterJ implements DelegationTokenTableDef, DelegationTokenDataAccess<HopDelegationToken> {

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
    public HopDelegationToken findBySeqNumber(int seqnumber) throws StorageException {
        HopsSession session = connector.obtainSession();

        DelegationTokenDTO delegationTokenDTO = null;
        if (session != null) {
            delegationTokenDTO = session.find(DelegationTokenDTO.class, seqnumber);
        }
        if (delegationTokenDTO == null) {
            throw new StorageException("HOP :: Error while retrieving delegationToken with seq_number=" + seqnumber);
        }

        return createHopDelegationToken(delegationTokenDTO);
    }

    @Override
    public void createDelegationTokenEntry(HopDelegationToken hopDelegationToken) throws StorageException {
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
    public void prepare(Collection<HopDelegationToken> modified, Collection<HopDelegationToken> removed) throws StorageException {
        HopsSession session = connector.obtainSession();
        try {
            if (removed != null) {
                List<DelegationTokenDTO> toRemove = new ArrayList<DelegationTokenDTO>();
                for (HopDelegationToken hop : removed) {
                    toRemove.add(session.newInstance(DelegationTokenClusterJ.DelegationTokenDTO.class, hop.getSeqnumber()));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<DelegationTokenDTO> toModify = new ArrayList<DelegationTokenDTO>();
                for (HopDelegationToken hop : modified) {
                    toModify.add(createPersistable(hop, session));
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    private HopDelegationToken createHopDelegationToken(DelegationTokenDTO delegationTokenDTO)
        throws StorageException {
      try {
        return new HopDelegationToken(delegationTokenDTO.getseqnumber(), CompressionUtils
            .decompress(delegationTokenDTO.getrmdtidentifier()));
      } catch (IOException e) {
        throw new StorageException(e);
      } catch (DataFormatException e) {
        throw new StorageException(e);
      }
    }

    private List<HopDelegationToken> createHopDelegationTokenList(List<DelegationTokenClusterJ.DelegationTokenDTO> list)
        throws StorageException {
        List<HopDelegationToken> hopList = new ArrayList<HopDelegationToken>();
        for (DelegationTokenClusterJ.DelegationTokenDTO dto : list) {
            hopList.add(createHopDelegationToken(dto));
        }
        return hopList;
    }

    private DelegationTokenDTO createPersistable(HopDelegationToken hop, HopsSession session) throws StorageException {
        DelegationTokenClusterJ.DelegationTokenDTO delegationTokenDTO = session.newInstance(DelegationTokenClusterJ.DelegationTokenDTO.class);
        delegationTokenDTO.setseqnumber(hop.getSeqnumber());
      try {
        delegationTokenDTO.setrmdtidentifier(
            CompressionUtils.compress(hop.getRmdtidentifier()));
      } catch (IOException e) {
        throw new StorageException(e);
      }

      return delegationTokenDTO;
    }
}