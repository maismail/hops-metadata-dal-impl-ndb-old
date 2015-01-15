/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import static se.sics.hop.metadata.hdfs.tabledef.LeaderTableDef.HTTP_ADDRESS;
import se.sics.hop.metadata.hdfs.tabledef.YarnLeaderTableDef;

/**
 *
 * @author gautier
 */
public class YarnLeaderClusterj extends LeaderClusterj implements YarnLeaderTableDef {

    @PersistenceCapable(table = TABLE_NAME)
    public interface YarnLeaderDTO extends LeaderDTO{

        @PrimaryKey
        @Column(name = ID)
        long getId();

        void setId(long id);

        @PrimaryKey
        @Column(name = PARTITION_VAL)
        int getPartitionVal();

        void setPartitionVal(int partitionVal);

        @Column(name = COUNTER)
        long getCounter();

        void setCounter(long counter);

        @Column(name = TIMESTAMP)
        long getTimestamp();

        void setTimestamp(long timestamp);

        @Column(name = HOSTNAME)
        String getHostname();

        void setHostname(String hostname);

        @Column(name = HTTP_ADDRESS)
        String getHttpAddress();

        void setHttpAddress(String httpAddress);
        
    }
   
    public YarnLeaderClusterj(){
        super(YarnLeaderDTO.class);
    }
}
