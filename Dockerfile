FROM fedora

RUN \
    dnf --assumeyes install \
        https://download1.rpmfusion.org/free/fedora/rpmfusion-free-release-$(rpm -E %fedora).noarch.rpm \
        https://download1.rpmfusion.org/nonfree/fedora/rpmfusion-nonfree-release-$(rpm -E %fedora).noarch.rpm && \
    dnf --assumeyes update && \
    dnf --assumeyes install \
        python3-pip \
        python3-devel \
        gcc \
        redhat-rpm-config \
        rocksdb-devel \
        gcc-c++ \
        lz4-devel \
        bzip2-devel \
        zlib-devel \
        snappy-devel && \
    dnf clean all

RUN \
    pip3.6 install pytest && \
    pip3.6 install aiohttp && \
    pip3.6 install aioredis && \
    pip3.6 install grpcio && \
    pip3.6 install hiredis && \
    pip3.6 install msgpack-python && \
    pip3.6 install psutil && \
    pip3.6 install pymongo && \
    pip3.6 install python-rocksdb && \
    pip3.6 install redis && \
    pip3.6 install uvloop==0.8.1
