#!/bin/bash

# Reference:
# https://github.com/pypa/python-manylinux-demo

set -e

PROJ=bigdict
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
    (cd /src/ && "${pybin}/python" -m build --sdist --wheel -o ${BUILDDIR})
done

for whl in ${BUILDDIR}/*.whl; do
    repair_wheel "$whl"
done

# Install packages and run tests
for pyver in "${pythonversions[@]}"; do
    echo
    echo ---- $pyver ----
    echo
    pybin=/opt/python/${pyver}/bin
    "${pybin}/pip" install ${PROJ} --no-index -f ${BUILDDIR}
    (cd "$HOME"; "${pybin}/python" -m pytest /src/tests) || exit 1
done

chown -R ${HOSTUSER} ${BUILDDIR}
