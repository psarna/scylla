#!/bin/bash -e

PRODUCT=$(cat SCYLLA-PRODUCT-FILE)

. /etc/os-release
print_usage() {
    echo "build_rpm.sh --rebuild-dep --reloc-pkg build/release/scylla-package.tar.gz"
    echo "  --dist  create a public distribution rpm"
    echo "  --xtrace print command traces before executing command"
    echo "  --reloc-pkg specify relocatable package path"
    exit 1
}
DIST=false
RELOC_PKG=
while [ $# -gt 0 ]; do
    case "$1" in
        "--dist")
            DIST=true
            shift 1
            ;;
        "--target") # This is obsolete, but I keep this in order not to break people's scripts.
            TARGET=$2
            shift 2
            ;;
        "--xtrace")
            set -o xtrace
            shift 1
            ;;
        "--reloc-pkg")
            RELOC_PKG=$2
            shift 2
            ;;
        *)
            print_usage
            ;;
    esac
done

is_redhat_variant() {
    [ -f /etc/redhat-release ]
}
pkg_install() {
    if is_redhat_variant; then
        sudo yum install -y $1
    else
        echo "Requires to install following command: $1"
        exit 1
    fi
}


if [ ! -e SCYLLA-RELOCATABLE-FILE ]; then
    echo "do not directly execute build_rpm.sh, use reloc/build_rpm.sh instead."
    exit 1
fi

if [ -z "$RELOC_PKG" ]; then
    print_usage
    exit 1
fi
if [ ! -f "$RELOC_PKG" ]; then
    echo "$RELOC_PKG is not found."
    exit 1
fi

if [ ! -f /usr/bin/rpmbuild ]; then
    pkg_install rpm-build
fi
if [ ! -f /usr/bin/git ]; then
    pkg_install git
fi
if [ ! -f /usr/bin/pystache ]; then
    if is_redhat_variant; then
        sudo yum install -y python2-pystache || sudo yum install -y pystache
    elif is_debian_variant; then
        sudo apt-get install -y python2-pystache
    fi
fi

RELOC_PKG_BASENAME=$(basename $RELOC_PKG)
SCYLLA_VERSION=$(cat SCYLLA-VERSION-FILE)
SCYLLA_RELEASE=$(cat SCYLLA-RELEASE-FILE)

RPMBUILD=$(readlink -f ../)
mkdir -p $RPMBUILD/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}

xz_thread_param=
if xz --help | grep -q thread; then
    # use as many threads as there are CPUs
    xz_thread_param="T$(nproc)"
fi

rpm_payload_opts=(--define "_binary_payload w2${xz_thread_param}.xzdio")

ln -fv $RELOC_PKG $RPMBUILD/SOURCES/
ln -fv dist/redhat/systemd.inc $RPMBUILD/SOURCES/
pystache dist/redhat/scylla.spec.mustache "{ \"version\": \"$SCYLLA_VERSION\", \"release\": \"$SCYLLA_RELEASE\", \"housekeeping\": $DIST, \"product\": \"$PRODUCT\", \"$PRODUCT\": true, \"reloc_pkg\": \"$RELOC_PKG_BASENAME\" }" > $RPMBUILD/SPECS/scylla.spec
rpmbuild -ba "${rpm_payload_opts[@]}" --define "_topdir $RPMBUILD" $RPMBUILD/SPECS/scylla.spec
