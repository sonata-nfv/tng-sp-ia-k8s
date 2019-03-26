"""
Copyright (c) 2017 5GTANGO
ALL RIGHTS RESERVED.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Neither the name of the 5GTANGO
nor the names of its contributors may be used to endorse or promote
products derived from this software without specific prior written
permission.

This work has been performed in the framework of the 5GTANGO project,
funded by the European Commission under Grant number 761493 through
the Horizon 2020 and 5G-PPP programmes. The authors would like to
acknowledge the contributions of their colleagues of the 5GTANGO
partner consortium (www.5gtango.eu).
"""
"""
This is SONATA's function lifecycle management plugin
"""

import os
from urllib.parse import urlparse

# List of topics that are used by the K8S Wrapper for its rabbitMQ communication

# With the FLM
NODE_LIST = "infrastructure.k8s.management.compute.list"

# With infrastructure adaptor
CNF_DEPLOY = 'infrastructure.k8s.function.deploy'
CNF_DEPLOY_RESPONSE = 'nbi.infrastructure.k8s.function.deploy'
CNF_FUNCTION_REMOVE = 'infrastructure.k8s.function.remove'
CNF_SERVICE_REMOVE = 'infrastructure.k8s.service.remove'
CNF_PREPARE = 'infrastructure.k8s.service.prepare'
CNF_CONFIGURE = 'infrastructure.k8s.function.configure'

# Catalogue urls
cat_path = os.environ.get("cat_path").strip("/")
vnfd_ext = os.environ.get("vnfd_collection").strip("/")

vnfd_path = cat_path + '/' + vnfd_ext

# Repository urls
repo_path = os.environ.get("repo_path").strip("/")
vnfr_ext = os.environ.get("vnfr_collection").strip("/")

vnfr_path = repo_path + '/' + vnfr_ext

# Monitoring urls
monitoring_path = os.environ.get("monitoring_path").strip("/")