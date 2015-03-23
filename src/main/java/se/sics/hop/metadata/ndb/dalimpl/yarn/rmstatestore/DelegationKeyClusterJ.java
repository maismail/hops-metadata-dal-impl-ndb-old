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
import se.sics.hop.metadata.hdfs.entity.yarn.rmstatestore.HopDelegationKey;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.rmstatestore.DelegationKeyDataAccess;
import se.sics.hop.metadata.yarn.tabledef.rmstatestore.DelegationKeyTableDef;
import se.sics.hop.util.CompressionUtils;

public class DelegationKeyClusterJ implements DelegationKeyTableDef, DelegationKeyDataAccess<HopDelegationKey> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface DelegationKeyDTO {

        @PrimaryKey
        @Column(name = KEY)
        int getkey();

        void setkey(int key);

        @Column(name = DELEGATIONKEY)
        byte[] getdelegationkey();

        void setdelegationkey(byte[] delegationkey);
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
  public void remove(HopDelegationKey removed) throws StorageException {
    HopsSession session = connector.obtainSession();
    session.deletePersistent(session.newInstance(
            DelegationKeyClusterJ.DelegationKeyDTO.class, removed.getKey()));
  }

    @Override
    public void createDTMasterKeyEntry(HopDelegationKey hopDelegationKey) throws StorageException{
        HopsSession session = connector.obtainSession();
        session.savePersistent(createPersistable(hopDelegationKey, session));
    }

    @Override
    public List<HopDelegationKey> getAll() throws StorageException {
        try {
            HopsSession session = connector.obtainSession();
            HopsQueryBuilder qb = session.getQueryBuilder();
            HopsQueryDomainType<DelegationKeyDTO> dobj = qb.createQueryDefinition(DelegationKeyDTO.class);
            HopsQuery<DelegationKeyDTO> query = session.createQuery(dobj);
            List<DelegationKeyDTO> results = query.getResultList();
            
                return createHopDelegationKeyList(results);
           
        } catch (Exception e) {
            throw new StorageException(e);
        }

    }

  private HopDelegationKey createHopDelegationKey(
          DelegationKeyDTO delegationKeyDTO)
          throws StorageException {
    try {
      return new HopDelegationKey(delegationKeyDTO.getkey(), CompressionUtils.
              decompress(delegationKeyDTO.getdelegationkey()));
    } catch (IOException e) {
      throw new StorageException(e);
    } catch (DataFormatException e) {
      throw new StorageException(e);
    }
  }

    private List<HopDelegationKey> createHopDelegationKeyList(List<DelegationKeyClusterJ.DelegationKeyDTO> list) throws StorageException {
        List<HopDelegationKey> hopList = new ArrayList<HopDelegationKey>();
        for (DelegationKeyClusterJ.DelegationKeyDTO dto : list) {
            hopList.add(createHopDelegationKey(dto));
        }
        return hopList;
    }

  private DelegationKeyDTO createPersistable(HopDelegationKey hop,
          HopsSession session) throws StorageException {
    DelegationKeyClusterJ.DelegationKeyDTO delegationKeyDTO = session.
            newInstance(DelegationKeyClusterJ.DelegationKeyDTO.class);
    delegationKeyDTO.setkey(hop.getKey());
    try {
      delegationKeyDTO.setdelegationkey(CompressionUtils.compress(hop.
              getDelegationkey()));
    } catch (IOException e) {
      throw new StorageException(e);
    }

    return delegationKeyDTO;
  }
}
