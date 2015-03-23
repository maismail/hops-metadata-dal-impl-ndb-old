
package io.hops.metadata.ndb.dalimpl.yarn.rmstatestore;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DataFormatException;

import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.entity.yarn.rmstatestore.HopSecretMamagerKey;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.dal.rmstatestore.SecretMamagerKeysDataAccess;
import io.hops.metadata.yarn.tabledef.rmstatestore.SecretMamagerKeysTableDef;
import io.hops.util.CompressionUtils;
import io.hops.metadata.ndb.ClusterjConnector;

public class SecretMamagerKeysClusterJ implements SecretMamagerKeysTableDef,
    SecretMamagerKeysDataAccess<HopSecretMamagerKey> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface SecretMamagerKeysDTO {

    @PrimaryKey
    @Column(name = KEYID)
    String getkeyid();

    void setkeyid(String keyid);

    @Column(name = KEY)
    byte[] getkey();

    void setkey(byte[] key);
  }
  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public List<HopSecretMamagerKey> getAll() throws StorageException {
 
      HopsSession session = connector.obtainSession();
      HopsQueryBuilder qb = session.getQueryBuilder();
      HopsQueryDomainType<SecretMamagerKeysDTO> dobj = qb.createQueryDefinition(SecretMamagerKeysDTO.class);
      HopsQuery<SecretMamagerKeysDTO> query = session.createQuery(dobj);
      List<SecretMamagerKeysDTO> results = query.getResultList();
      return createHopSecretMamagerKeyList(results);
    
  }

  @Override
  public void add(HopSecretMamagerKey toAdd) throws StorageException {
    HopsSession session = connector.obtainSession();
    session.savePersistent(createPersistable(toAdd, session));
  }

  @Override
  public void remove(HopSecretMamagerKey toRemove) throws StorageException {
    HopsSession session = connector.obtainSession();
    session.deletePersistent(session.newInstance(SecretMamagerKeysDTO.class,
            toRemove.getKeyType()));
  }
  
  private SecretMamagerKeysDTO createPersistable(HopSecretMamagerKey hop,
          HopsSession session) throws StorageException {
    SecretMamagerKeysDTO keyDTO = session.
            newInstance(SecretMamagerKeysDTO.class);
    keyDTO.setkeyid(hop.getKeyType());
    try {
      keyDTO.setkey(CompressionUtils.compress(hop.getKey()));
    } catch (IOException e) {
      throw new StorageException(e);
    }

    return keyDTO;
  }

  private HopSecretMamagerKey createHopSecretMamagerKey(
          SecretMamagerKeysDTO keyDTO) throws StorageException {
    if (keyDTO != null) {
      try {
        return new HopSecretMamagerKey(keyDTO.getkeyid(),
                CompressionUtils.decompress(keyDTO.getkey()));
      } catch (IOException e) {
        throw new StorageException(e);
      } catch (DataFormatException e) {
        throw new StorageException(e);
      }
    } else {
      return null;
    }
  }
  
   private List<HopSecretMamagerKey> createHopSecretMamagerKeyList(List<SecretMamagerKeysDTO> list) throws StorageException {
        List<HopSecretMamagerKey> hopList = new ArrayList<HopSecretMamagerKey>();
        for (SecretMamagerKeysDTO dto : list) {
            hopList.add(createHopSecretMamagerKey(dto));
        }
        return hopList;

    }
}
