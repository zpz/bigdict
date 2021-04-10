ARG PYTHON_VERSION=3.8

#################################################
FROM python:${PYTHON_VERSION}-slim AS build
USER root

COPY install-deps /opt/bigdict/
RUN cd /opt/bigdict && ./install-deps


#################################################
FROM build AS test

COPY requirements-test.txt /opt/bigdict/
RUN cd /opt/bigdict \
    && python -m pip install --no-cache-dir -r requirements-test.txt

COPY ./src /opt/bigdict/src
COPY setup.py setup.cfg README.md /opt/bigdict/

RUN cd /opt/bigdict && python -m pip install --no-cache-dir -q .

COPY tests /opt/bigdict/tests


#################################################
FROM build AS release-prep

COPY ./src /opt/bigdict/src
COPY setup.py setup.cfg README.md /opt/bigdict/

RUN mkdir /bigdict-dist \
        && cd /opt/bigdict \
        && cp install-deps /bigdict-dist/ \
        && python setup.py sdist -d /bigdict-dist \
        && python setup.py bdist_wheel -d /bigdict-dist


#################################################
FROM busybox:1 AS release

COPY --from=release-prep /bigdict-dist /bigdict-dist
RUN echo '#!/bin/sh' > /bigdict-dist/INSTALL \
        && echo './install-deps && python -m pip install --no-cache-dir --no-index --find-links ./ bigdict' >> /bigdict-dist/INSTALL \
        && chmod +x /bigdict-dist/INSTALL


#################################################
FROM python:${PYTHON_VERSION}-slim AS release-test

COPY --from=release /bigdict-dist /tmp/bigdict-dist
RUN cd /tmp/bigdict-dist && ./INSTALL

# prep for tests
COPY tests /tmp/bigdict-dist/tests
COPY requirements-test.txt /tmp/bigdict-dist/
RUN cd /tmp/bigdict-dist \
        && python -m pip install --no-cache-dir -r requirements-test.txt
