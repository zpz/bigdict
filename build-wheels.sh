#!/bin/bash

# Reference:
# https://github.com/pypa/python-manylinux-demo

set -e

PLAT=manylinux2010_x86_64

function repair_wheel {
    wheel="$1"
    if ! auditwheel show "$wheel"; then
        echo "Skipping non-platform wheel $wheel"
    else
        auditwheel repair "$wheel" --plat "$PLAT" -w ${BUILDDIR}
    fi
}


for whl in ${BUILDDIR}/*.whl; do
    repair_wheel "$whl"
done


# Install package and run tests
pythonversions=( $PYTHONVERSIONS )
for pyver in "${pythonversions[@]}"; do
    echo
    echo ---- testing in $pyver ----
    echo
    pybin=/opt/python/${pyver}/bin
    "${pybin}/pip" install ${PROJ} --no-index -f ${BUILDDIR}
    (cd "$HOME"; "${pybin}/python" -m pytest /src/tests)
done
