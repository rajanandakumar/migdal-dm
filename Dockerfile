FROM gitlab-registry.cern.ch/linuxsupport/cc7-base:latest
MAINTAINER Raja Nandakumar

RUN yum -y update && yum -y groupinstall "Development Tools"
RUN yum install -y wget vim-enhanced vim-minimal

RUN cd /etc/yum.repos.d && wget http://linuxsoft.cern.ch/wlcg/wlcg-centos7.repo \
    && wget http://repository.egi.eu/sw/production/cas/1/current/repo-files/EGI-trustanchors.repo
RUN cd /etc/pki/rpm-gpg && wget http://linuxsoft.cern.ch/wlcg/RPM-GPG-KEY-wlcg
RUN yum update

RUN yum install -y gsi-openssh voms-clients-cpp 
RUN yum install -y lcg-tags lcgdm-devel lcgdm-libs lcg-info lcg-ManageVOTag lcg-infosites
RUN yum install -y edg-* ca_policy_*
RUN yum update

# COPY cvmfs-release-latest.noarch.rpm .
RUN yum install -y https://ecsft.cern.ch/dist/cvmfs/cvmfs-release/cvmfs-release-latest.noarch.rpm
RUN yum install -y cvmfs

# RUN mkdir -p /home/nraja/.globus
# COPY usercert-20220507.pem /home/nraja/.globus/usercert.pem
# COPY userkey-20220507.pem /home/nraja/.globus/userkey.pem
# RUN chmod 400 /home/nraja/.globus/*

