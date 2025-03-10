FROM apache/hadoop:3.4.1

USER root

# Actualizar los repositorios de CentOS
RUN mv /etc/yum.repos.d/CentOS-Base.repo /etc/yum.repos.d/CentOS-Base.repo.bak

RUN cat <<EOF > /etc/yum.repos.d/CentOS-Base.repo
[base]
name=CentOS-7 - Base
baseurl=http://vault.centos.org/centos/7/os/x86_64/
enabled=1
gpgcheck=0

[updates]
name=CentOS-7 - Updates
baseurl=http://vault.centos.org/centos/7/updates/x86_64/
enabled=1
gpgcheck=0

[extras]
name=CentOS-7 - Extras
baseurl=http://vault.centos.org/centos/7/extras/x86_64/
enabled=1
gpgcheck=0
EOF

RUN yum clean all
RUN yum makecache

# Instalar Java y Python 3
RUN yum install -y java-11-openjdk-devel python3 && \
    yum clean all

# Definir variables de entorno
ENV JAVA_HOME=/usr/lib/jvm/jre
ENV PYTHON=/usr/bin/python3

CMD ["/bin/bash"]