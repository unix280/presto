echo "===================================================== Starting installHiveDep.sh ====================================================="
PRESTO_DIR=`pwd`
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