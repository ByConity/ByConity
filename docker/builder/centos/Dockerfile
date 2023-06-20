FROM centos:centos7
USER root
RUN yum update -y && yum makecache
RUN yum install -y git swig unzip openssh-server  gzip bash  binutils  bzip2 coreutils  diffutils findutils grep hostname ncurses-base passwd perl procps psmisc python3 sed tar tzdata util-linux curl make

RUN yum install centos-release-scl -y 
RUN yum install devtoolset-7-gcc* -y 

WORKDIR /home

COPY foundationdb-clients-7.1.27-1.el7.x86_64.rpm ./
RUN yum localinstall -y foundationdb-clients-7.1.27-1.el7.x86_64.rpm

COPY cmake-3.25.3-linux-x86_64.sh ./
RUN ./cmake-3.25.3-linux-x86_64.sh --skip-license --prefix=/usr/local/ 

COPY ninja-linux.zip ./
RUN unzip ninja-linux.zip && mv ninja /usr/local/bin/

COPY llvm-project-11.1.0.src.tar.xz ./

RUN yum install -y openssl-devel
RUN tar -xf llvm-project-11.1.0.src.tar.xz && source /opt/rh/devtoolset-7/enable && cd llvm-project-11.1.0.src && cmake -S llvm -B build -G Ninja -DLLVM_ENABLE_PROJECTS="clang;lld;compiler-rt" -DCMAKE_BUILD_TYPE=Release && cmake --build ./build/ --target install 

COPY build.sh /
RUN rm -rf ninja-linux.zip foundationdb-clients-7.1.27-1.el7.x86_64.rpm cmake-3.25.3-linux-x86_64.sh llvm-project-11.1.0.src.tar.xz llvm-project-11.1.0.src
CMD ["/bin/bash", "/build.sh"]

