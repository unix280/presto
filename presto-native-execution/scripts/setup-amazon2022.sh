set -e -o pipefail

function install_prebuilt_packages {
  dnf install -y \
    unzip \
    wget \
    g++ \
    bison \
    flex \
    tar \
    gperf \
    python3 \
    vim \
    git \
    cmake \
    patch \
    ninja-build \
    boost-devel \
    libuuid-devel \
    snappy-devel \
    lz4-devel \
    lzo-devel \
    libzstd-devel \
    xz-devel \
    bzip2-devel \
    libevent-devel \
    protobuf-devel \
    openssl-devel \
    libcurl-devel
}

SCRIPTDIR=$(dirname "${BASH_SOURCE[0]}")

# github_checkout $REPO $VERSION $GIT_CLONE_PARAMS clones or re-uses an existing clone of the
# specified repo, checking out the requested version.
function github_checkout {
  local REPO=$1
  shift
  local VERSION=$1
  shift
  local GIT_CLONE_PARAMS=$@
  local DIRNAME=$(basename $REPO)
  cd "${DEPENDENCY_DIR}"
  if [ -z "${DIRNAME}" ]; then
    echo "Failed to get repo name from ${REPO}"
    exit 1
  fi
  if [ -d "${DIRNAME}" ] && prompt "${DIRNAME} already exists. Delete?"; then
    rm -rf "${DIRNAME}"
  fi
  if [ ! -d "${DIRNAME}" ]; then
    git clone -q -b $VERSION $GIT_CLONE_PARAMS "https://github.com/${REPO}.git"
  fi
  cd "${DIRNAME}"
}


# get_cxx_flags [$CPU_ARCH]
# Sets and exports the variable VELOX_CXX_FLAGS with appropriate compiler flags.
# If $CPU_ARCH is set then we use that else we determine best possible set of flags
# to use based on current cpu architecture.
# The goal of this function is to consolidate all architecture specific flags to one
# location.
# The values that CPU_ARCH can take are as follows:
#   arm64  : Target Apple silicon.
#   aarch64: Target general 64 bit arm cpus.
#   avx:     Target Intel CPUs with AVX.
#   sse:     Target Intel CPUs with sse.
# Echo's the appropriate compiler flags which can be captured as so
# CXX_FLAGS=$(get_cxx_flags) or
# CXX_FLAGS=$(get_cxx_flags "avx")

function get_cxx_flags {
  local CPU_ARCH=$1

  local OS
  OS=$(uname)
  local MACHINE
  MACHINE=$(uname -m)

  if [ -z "$CPU_ARCH" ]; then

    if [ "$OS" = "Darwin" ]; then

      if [ "$MACHINE" = "x86_64" ]; then
        local CPU_CAPABILITIES
        CPU_CAPABILITIES=$(sysctl -a | grep machdep.cpu.features | awk '{print tolower($0)}')

        if [[ $CPU_CAPABILITIES =~ "avx" ]]; then
          CPU_ARCH="avx"
        else
          CPU_ARCH="sse"
        fi

      elif [[ $(sysctl -a | grep machdep.cpu.brand_string) =~ "Apple" ]]; then
        # Apple silicon.
        CPU_ARCH="arm64"
      fi
    else [ "$OS" = "Linux" ];

      local CPU_CAPABILITIES
      CPU_CAPABILITIES=$(cat /proc/cpuinfo | grep flags | head -n 1| awk '{print tolower($0)}')

      if [[ "$CPU_CAPABILITIES" =~ "avx" ]]; then
            CPU_ARCH="avx"
      elif [[ "$CPU_CAPABILITIES" =~ "sse" ]]; then
            CPU_ARCH="sse"
      elif [ "$MACHINE" = "aarch64" ]; then
            CPU_ARCH="aarch64"
      fi
    fi
  fi

  case $CPU_ARCH in

    "arm64")
      echo -n "-mcpu=apple-m1+crc -std=c++17"
    ;;

    "avx")
      echo -n "-mavx2 -mfma -mavx -mf16c -mlzcnt -std=c++17"
    ;;

    "sse")
      echo -n "-msse4.2 -std=c++17"
    ;;

    "aarch64")
      echo -n "-march=armv8-a+crc+crypto -std=c++17"
    ;;
  *)
    echo -n "Architecture not supported!"
  esac

}

function cmake_install {
  local NAME=$(basename "$(pwd)")
  local BINARY_DIR=_build
  if [ -d "${BINARY_DIR}" ] && prompt "Do you want to rebuild ${NAME}?"; then
    rm -rf "${BINARY_DIR}"
  fi
  mkdir -p "${BINARY_DIR}"
  CPU_TARGET="${CPU_TARGET:-avx}"
  COMPILER_FLAGS=$(get_cxx_flags $CPU_TARGET)

  # CMAKE_POSITION_INDEPENDENT_CODE is required so that Velox can be built into dynamic libraries \
  cmake -Wno-dev -B"${BINARY_DIR}" \
    -GNinja \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DCMAKE_CXX_STANDARD=17 \
    "${INSTALL_PREFIX+-DCMAKE_PREFIX_PATH=}${INSTALL_PREFIX-}" \
    "${INSTALL_PREFIX+-DCMAKE_INSTALL_PREFIX=}${INSTALL_PREFIX-}" \
    -DCMAKE_CXX_FLAGS="$COMPILER_FLAGS" \
    -DBUILD_TESTING=OFF \
    "$@"
  ninja -C "${BINARY_DIR}" install
}

# Folly must be built with the same compiler flags so that some low level types
# are the same size.
CPU_TARGET="${CPU_TARGET:-avx}"
export COMPILER_FLAGS=$(get_cxx_flags $CPU_TARGET)
FB_OS_VERSION=v2022.03.14.00
NPROC=${NPROC:-$(getconf _NPROCESSORS_ONLN)}
DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)}

function wget_and_untar {
  local URL=$1
  local DIR=$2
  mkdir -p "${DIR}" &&
  wget -q --max-redirect 3 -O - "${URL}" | tar -xz -C "${DIR}" --strip-components=1 > /dev/null
}

function make_install {
  make "-j${NPROC}" && make install > /dev/null
}

function install_fmt {
  github_checkout fmtlib/fmt 8.0.0
  cmake_install -DFMT_TEST=OFF
}

function install_folly {
  github_checkout facebook/folly "${FB_OS_VERSION}"
  patch -p1 < ../patches/apply_folly.patch
  cmake_install -DBUILD_TESTS=OFF
}

function install_glog {
  wget_and_untar https://github.com/google/glog/archive/refs/tags/v0.5.0.tar.gz glog
  cd glog &&
  cmake_install -DBUILD_SHARED_LIBS=ON 
}

function install_gflags {
  wget_and_untar https://github.com/gflags/gflags/archive/refs/tags/v2.2.2.tar.gz gflags
  cd gflags &&
  cmake_install -DBUILD_SHARED_LIBS=ON -DBUILD_STATIC_LIBS=ON -DBUILD_gflags_LIB=ON -DLIB_SUFFIX=64
}

function install_re2 {
  wget_and_untar https://github.com/google/re2/archive/refs/tags/2021-11-01.tar.gz re2
  cd re2 &&
  make_install
}

function install_antlr4_runtime {
  wget https://www.antlr.org/download/antlr4-cpp-runtime-4.9.3-source.zip
  mkdir -p antlr4-cpp-runtime-4.9.3-source &&
  cd antlr4-cpp-runtime-4.9.3-source &&
  unzip ../antlr4-cpp-runtime-4.9.3-source.zip
  mkdir -p _build && mkdir -p run && cd _build &&
  cmake ..
  DESTDIR=../run make "-j${NPROC}" install
  cp -r ../run/usr/local/include/antlr4-runtime  /usr/local/include/
  cp ../run/usr/local/lib/libantlr*  /usr/local/lib/
  ldconfig
}

function install_libsodium {
  wget_and_untar https://download.libsodium.org/libsodium/releases/LATEST.tar.gz libsodium
  cd libsodium &&
  ./configure &&
  make_install
}

function install_fizz {
  github_checkout facebookincubator/fizz "${FB_OS_VERSION}"
  cmake_install -DBUILD_TESTS=OFF -S fizz
}

function install_wangle {
  github_checkout facebook/wangle "${FB_OS_VERSION}"
  cmake_install -DBUILD_TESTS=OFF -S wangle
}

function install_proxygen { 
  github_checkout facebook/proxygen "${FB_OS_VERSION}"
  cmake_install -DBUILD_SAMPLES=OFF -DBUILD_TESTS=OFF
}

function install_fbthrift {
  github_checkout facebook/fbthrift "${FB_OS_VERSION}"
  cmake_install -DBUILD_TESTS=OFF
}

function install_aws_sdk_cpp {
  local AWS_REPO_NAME="aws/aws-sdk-cpp"
  local AWS_SDK_VERSION="1.9.96"

  github_checkout $AWS_REPO_NAME $AWS_SDK_VERSION --depth 1 --recurse-submodules
  patch -p1 < ../patches/apply_aws_sdk_cpp.patch
  cmake_install -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS:BOOL=OFF -DMINIMIZE_SIZE:BOOL=ON -DENABLE_TESTING:BOOL=OFF -DBUILD_ONLY:STRING="s3;identity-management"
}

function install_double_conversion {
  github_checkout google/double-conversion v3.1.5
  cmake_install -DBUILD_SHARED_LIBS=ON -DBUILD_TESTING=OFF
}

function run_and_time {
  echo "Starting $1"
  (time "$@")
  { echo "+ Finished running $*"; } 2> /dev/null
}

function install_prestocpp_deps {
  install_prebuilt_packages
  SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
  DEPENDENCY_DIR=${SCRIPT_DIR}/../third-party
  echo ${DEPENDENCY_DIR}
  mkdir -p ${DEPENDENCY_DIR} && cd ${DEPENDENCY_DIR}
  if [ ! -d /usr/local/include/gflags ]; then
    run_and_time install_gflags
  fi
  if [ ! -d /usr/local/include/glog ]; then
    run_and_time install_glog
  fi
  if [ ! -d /usr/local/include/fmt ]; then
    run_and_time install_fmt
  fi
  if [ ! -d /usr/local/include/double-conversion ]; then
    run_and_time install_double_conversion
  fi
  if [ ! -d /usr/local/include/re2 ]; then
    run_and_time install_re2
  fi
  if [ ! -d /usr/local/include/antlr4-runtime ]; then
    run_and_time install_antlr4_runtime
  fi
  if [ ! -f /usr/local/include/sodium.h ]; then
    run_and_time install_libsodium
  fi
  if [ ! -d /usr/local/include/folly ]; then
    run_and_time install_folly
  fi
  if [ ! -d /usr/local/include/fizz ]; then
    run_and_time install_fizz
  fi
  if [ ! -d /usr/local/include/wangle ]; then
    run_and_time install_wangle
  fi
  if [ ! -d /usr/local/include/proxygen ]; then
    run_and_time install_proxygen 
  fi
  if [ ! -d /usr/local/include/thrift ]; then
    run_and_time install_fbthrift
  fi
  if [ ! -d /usr/local/include/aws ]; then
    run_and_time install_aws_sdk_cpp
  fi
}

(return 2> /dev/null) && return # If script was sourced, don't run commands.

(
  if [[ $# -ne 0 ]]; then
    for cmd in "$@"; do
      run_and_time "${cmd}"
    done
  else
    install_prestocpp_deps
  fi
)
