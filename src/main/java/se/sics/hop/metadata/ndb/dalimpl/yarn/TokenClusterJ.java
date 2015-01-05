package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopToken;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.TokenDataAccess;
import se.sics.hop.metadata.yarn.tabledef.TokenTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class TokenClusterJ implements TokenTableDef, TokenDataAccess<HopToken> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface TokenDTO {

        @PrimaryKey
        @Column(name = ID)
        int getid();

        void setid(int id);

        @Column(name = IDENTIFIER)
        byte[] getidentifier();

        void setidentifier(byte[] identifier);

        @Column(name = KIND)
        String getkind();

        void setkind(String kind);

        @Column(name = PASSWORD)
        byte[] getpassword();

        void setpassword(byte[] password);

        @Column(name = SERVICE)
        String getservice();

        void setservice(String service);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopToken findById(int id) throws StorageException {
        HopsSession session = connector.obtainSession();

        TokenDTO tokenDTO = null;
        if (session != null) {
            tokenDTO = session.find(TokenDTO.class, id);
        }
        if (tokenDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopToken(tokenDTO);
    }

    @Override
    public void prepare(Collection<HopToken> modified, Collection<HopToken> removed) throws StorageException {
        HopsSession session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopToken token : removed) {

                    TokenDTO persistable = session.newInstance(TokenDTO.class, token.getId());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopToken token : modified) {
                    TokenDTO persistable = createPersistable(token, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createToken(HopToken token) throws StorageException {
        HopsSession session = connector.obtainSession();
        createPersistable(token, session);
    }

    private HopToken createHopToken(TokenDTO tokenDTO) {
        return new HopToken(
                tokenDTO.getid(),
                tokenDTO.getidentifier(),
                tokenDTO.getkind(),
                tokenDTO.getpassword(),
                tokenDTO.getservice());
    }

    private TokenDTO createPersistable(HopToken token, HopsSession session) throws StorageException {
        TokenDTO tokenDTO = session.newInstance(TokenDTO.class);
        //Set values to persist new rmnode
        tokenDTO.setid(token.getId());
        tokenDTO.setidentifier(token.getIdentifier());
        tokenDTO.setkind(token.getKind());
        tokenDTO.setpassword(token.getPassword());
        tokenDTO.setservice(token.getService());
        return tokenDTO;
    }
}
