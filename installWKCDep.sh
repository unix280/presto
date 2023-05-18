echo "===================================================== Starting installWKCDep.sh ====================================================="
PRESTO_DIR=`pwd`
cd $PRESTO_DIR/..
WKC_LIB_DIR="wkc-libs"
if [ -d "$WKC_LIB_DIR" ]; then
  echo "IBM WKC Lib Repo already available in ${WKC_LIB_DIR}..."
else
  echo "Cloning IBM WKC Lib Repo..."
  git clone git@github.ibm.com:lakehouse/wkc-libs.git
fi
cd wkc-libs/
chmod 755 *
echo "Starting JAR Installation .m2"
./mvnw install:install-file -Dfile=libs/lts-masking-0.3.9.jar -DgroupId=com.ibm.dp -DartifactId=lts-masking -Dversion=0.3.9 -Dpackaging=jar -DgeneratePom=true
./mvnw install:install-file -Dfile=libs/wdp-policy-service-sdk-3.5.1227.jar -DgroupId=com.ibm.wdp.policy -DartifactId=wdp-policy-service-sdk -Dversion=3.5.1227 -Dpackaging=jar -DgeneratePom=true
echo "presto-wkc-dep JAR installed to .m2"