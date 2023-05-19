echo "===================================================== Starting installHiveDep.sh ====================================================="
echo "===================================================== Start building presto-hive4-apache ====================================================="

PRESTO_DIR=`pwd`
echo "^^^^^^^ current working dir ^^^^^^^^^^^^^"
pwd
echo $PRESTO_DIR
echo "^^^^^^^ current working dir ^^^^^^^^^^^^^"
cd $PRESTO_DIR/..
HIVE_DIR="presto-hive4-apache"
if [ -d "$HIVE_DIR" ]; then
  echo "IBM Hive Apache Repo already available in ${HIVE_DIR}..."
else
  echo "Cloning IBM Hive Apache Repo..."
  git clone git@github.ibm.com:lakehouse/presto-hive4-apache.git
fi
cd presto-hive4-apache/
echo "Building presto-hive4-apache..."
./mvnw clean install
echo "Cleaning up the hive-apache-ibm JAR..."
./refactorJar.sh
./mvnw install:install-file -Dfile=target/hive-apache-ibm-3.0.0-8.jar -DgroupId=com.facebook.presto.hive -DartifactId=hive-apache-ibm -Dversion=3.0.0-8 -Dpackaging=jar -DgeneratePom=true
echo "hive-apache-ibm JAR installed to .m2"
echo "===================================================== Start building presto-hadoop-apache2 ====================================================="
echo "^^^^^^^ current working dir 2 ^^^^^^^^^^^^^"
pwd
echo "^^^^^^^ current working dir 2^^^^^^^^^^^^^"
cd $PRESTO_DIR/..
echo "^^^^^^^ current working dir 3 ^^^^^^^^^^^^^"
pwd
echo "^^^^^^^ current working dir 3^^^^^^^^^^^^^"
HADOOP_DIR="presto-hadoop-apache2"
if [ -d "$HADOOP_DIR" ]; then
  echo "Custom Hadoop Apache2 Repo already available in ${HADOOP_DIR}..."
else
  echo "Cloning Custom Hadoop Apache2 Repo..."
  git clone git@github.ibm.com:lakehouse/presto-hadoop-apache2.git
fi
cd presto-hadoop-apache2/
echo "Building presto-hadoop-apache2..."
./mvnw clean install -DskipTests
./mvnw install:install-file -Dfile=target/hadoop-apache2-2.7.4-9.4.jar -DgroupId=com.facebook.presto.hadoop -DartifactId=hadoop-apache2 -Dversion=2.7.4-9.4 -Dpackaging=jar -DgeneratePom=true
echo "presto-hadoop-apache2 JAR installed to .m2"
