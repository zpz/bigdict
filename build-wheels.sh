#!/bin/bash

# Reference:
# https://github.com/pypa/python-manylinux-demo

set -e

PLAT=manylinux2010_x86_64
BUILDDIR=/build

function repair_wheel {
    wheel="$1"
    if ! auditwheel show "$wheel"; then
        echo "Skipping non-platform wheel $wheel"
    else
        auditwheel repair "$wheel" --plat "$PLAT" -w ${BUILDDIR}
    fi
}


pythonversions=( $PYTHONVERSIONS )

for pyver in "${pythonversions[@]}"; do
    echo
    echo ---- $pyver ----
    echo
    pybin=/opt/python/${pyver}/bin
    # "${pybin}/pip" wheel /src/ --no-deps -w ${BUILDDIR}
    (cd /src/ && "${pybin}/python" -m build --sdist --wheel -o ${BUILDDIR})
done

for whl in ${BUILDDIR}/*.whl; do
    repair_wheel "$whl"
done

# Install packages and test
for pyver in "${pythonversions[@]}"; do
    echo
    echo ---- $pyver ----
    echo
    pybin=/opt/python/${pyver}/bin
    "${pybin}/pip" install bigdict --no-index -f ${BUILDDIR}
    (cd "$HOME"; "${pybin}/python" -m pytest /src/tests)
done

chown -R ${HOSTUSER} ${BUILDDIR}
