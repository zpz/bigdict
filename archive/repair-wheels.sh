#!/bin/bash

# Reference:
# https://github.com/pypa/python-manylinux-demo

set -e

PLAT=manylinux2010_x86_64

for wheel in ${BUILDDIR}/*.whl; do
    if ! auditwheel show "$wheel"; then
        echo "Skipping non-platform wheel $wheel"
    else
        auditwheel repair "$wheel" --plat "$PLAT" -w ${BUILDDIR}
    fi
done
