package cea

import org.apache.spark.rdd.RDD

class ClusterProfilesModel (val profiles: RDD[ClusterProfile]) extends java.io.Serializable {
    def getProfiles = profiles
}