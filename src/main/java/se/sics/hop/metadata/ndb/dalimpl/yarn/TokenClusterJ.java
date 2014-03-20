package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopToken;
import se.sics.hop.metadata.ndb.ClusterjConnector;
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

        void setkind(int kind);

        @Column(name = PASSWORD)
        byte[] getpassword();

        void setpassword(byte[] password);

        @Column(name = SERVICE)
        String getservice();

        void setservice(int service);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopToken findById(int id) throws StorageException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void prepare(Collection<HopToken> modified, Collection<HopToken> removed) throws StorageException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void createToken(HopToken token) throws StorageException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
