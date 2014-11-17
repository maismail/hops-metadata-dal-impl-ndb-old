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
import java.util.Collection;
import java.util.List;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.rmstatestore.HopSecretMamagerKey;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.rmstatestore.SecretMamagerKeysDataAccess;
import se.sics.hop.metadata.yarn.tabledef.rmstatestore.SecretMamagerKeysTableDef;

/**
 *
 * @author gautier
 */
public class SecretMamagerKeysClusterJ implements SecretMamagerKeysTableDef, SecretMamagerKeysDataAccess<HopSecretMamagerKey> {

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
  public HopSecretMamagerKey findByKeyId(String id) throws StorageException {
    Session session = connector.obtainSession();

    SecretMamagerKeysDTO secretMamagerKeysDTO = null;
    if (session != null) {
      secretMamagerKeysDTO = session.find(SecretMamagerKeysDTO.class, id);
    }

    return createHopSecretMamagerKey(secretMamagerKeysDTO);
  }

  @Override
  public List<HopSecretMamagerKey> getAll() throws StorageException{
 
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<SecretMamagerKeysDTO> dobj = qb.createQueryDefinition(SecretMamagerKeysDTO.class);
      Query<SecretMamagerKeysDTO> query = session.createQuery(dobj);
      List<SecretMamagerKeysDTO> results = query.getResultList();
      return createHopSecretMamagerKeyList(results);
    
  }

  @Override
  public void prepare(Collection<HopSecretMamagerKey> modified, Collection<HopSecretMamagerKey> removed) throws StorageException {
    Session session = connector.obtainSession();
    try {
      if (removed != null) {
        List<SecretMamagerKeysDTO> toRemove = new ArrayList<SecretMamagerKeysDTO>();
        for (HopSecretMamagerKey hop : removed) {
          toRemove.add(session.newInstance(SecretMamagerKeysDTO.class, hop.getKeyId()));
        }
        session.deletePersistentAll(toRemove);
      }
      if (modified != null) {
        List<SecretMamagerKeysDTO> toModify = new ArrayList<SecretMamagerKeysDTO>();
        for (HopSecretMamagerKey hop : modified) {
          toModify.add(createPersistable(hop, session));
        }
        session.savePersistentAll(toModify);
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private SecretMamagerKeysDTO createPersistable(HopSecretMamagerKey hop, Session session) {
    SecretMamagerKeysDTO keyDTO = session.newInstance(SecretMamagerKeysDTO.class);
    keyDTO.setkeyid(hop.getKeyId());
    keyDTO.setkey(hop.getKey());

    return keyDTO;
  }

  private HopSecretMamagerKey createHopSecretMamagerKey(SecretMamagerKeysDTO keyDTO) {
    if (keyDTO != null) {
      return new HopSecretMamagerKey(keyDTO.getkeyid(),
              keyDTO.getkey());
    } else {
      return null;
    }
  }
  
   private List<HopSecretMamagerKey> createHopSecretMamagerKeyList(List<SecretMamagerKeysDTO> list) {
        List<HopSecretMamagerKey> hopList = new ArrayList<HopSecretMamagerKey>();
        for (SecretMamagerKeysDTO dto : list) {
            hopList.add(createHopSecretMamagerKey(dto));
        }
        return hopList;

    }
}
