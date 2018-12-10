# Copyright (c) 2017 5GTANGO
# ALL RIGHTS RESERVED.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Neither the name of the SONATA-NFV, 5GTANGO
# nor the names of its contributors may be used to endorse or promote
# products derived from this software without specific prior written
# permission.
#
#
# This work has been performed in the framework of the 5GTANGO project,
# funded by the European Commission under Grant number 761493 through
# the Horizon 2020 and 5G-PPP programmes. The authors would like to
# acknowledge the contributions of their colleagues of the 5GTANGO
# partner consortium (www.5gtango.eu).

FROM python:3.4-slim
LABEL organization=5GTANGO

# Configuration
ENV broker_host amqp://guest:guest@broker:5672/%2F
# TODO CHANGE EXACHANGE TO NEW ONE CREATED FOR IA
ENV broker_exchange son-kernel-ia

# Catalogue information
ENV cat_path http://localhost:4011/api/catalogues/v2
ENV vnfd_collection vnfs

# Repository information
ENV repo_path http://son-catalogue-repos:4011/records
ENV vnfr_collection vnfr/vnf-instances

# Monitoring information
ENV monitoring_path http://son-monitor-manager:8000/api/v1

# ENV Postgres
ENV POSTGRES_USER sonata
ENV POSTGRES_PASSWORD sonatatest
ENV DATABASE_HOST son-postgres
ENV DATABASE_PORT 5432

RUN apt-get update && apt-get install -y glpk-utils && apt-get install -y python3-pip libffi-dev libssl-dev
RUN apt-get install -y git
RUN pip install git+git://github.com/eandersson/amqpstorm.git@feature/reuse_channels

# add plugin related files
WORKDIR /
ADD README.md /tng-sp-ia-k8s/
ADD setup.py  /tng-sp-ia-k8s/

# install actual plugin
WORKDIR /tng-sp-ia-k8s
RUN python setup.py develop

ADD .  /tng-sp-ia-k8s

ENV KUBECONFIG /tng-sp-ia-k8s/config

CMD ["tng-sp-ia-k8s"]


