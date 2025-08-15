FROM mcr.microsoft.com/devcontainers/cpp:latest

RUN apt update \
 && apt install -y \
    clang-format \
 && rm -rf /var/lib/apt/lists/*

USER vscode

RUN cd ~ \
 && git clone --branch stable https://github.com/rui314/mold.git \
 && cd mold \
 && sudo ./install-build-deps.sh \
 && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=c++ -B build \
 && cmake --build build -j$(nproc) \
 && sudo cmake --build build --target install \
 && sudo ln -fs /usr/local/bin/mold /usr/bin/ld

RUN cd ~ \
 && git clone https://github.com/microsoft/vcpkg.git \
 && sh ./vcpkg/scripts/bootstrap.sh -disableMetrics

ENV VCPKG_TOOLCHAIN_PATH=/home/vscode/vcpkg/scripts/buildsystems/vcpkg.cmake

RUN curl https://sh.rustup.rs | sh -s -- -y

RUN sudo apt update \
 && sudo apt install -y \
    bison \
    flex \
    libreadline-dev \
 && sudo rm -rf /var/lib/apt/lists/*

ENV PATH="/usr/local/pgsql/bin:${PATH}"
RUN cd ~ \
 && curl https://ftp.postgresql.org/pub/source/v17.2/postgresql-17.2.tar.bz2 | bzip2 -d | tar x \
 && cd postgresql-17.2 \
 && ./configure --enable-cassert --enable-debug CFLAGS='-ggdb3' \
 && make -j$(nproc) \
 && sudo make install
