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

from kubernetes import client, config
from kubernetes.client.configuration import Configuration
from kubernetes.config import kube_config
from kubernetes.client.rest import ApiException
import logging
import threading
import concurrent.futures as pool
import uuid
import time
import os
import re
from os import path
import yaml, json
import uuid
from pprint import pprint
from copy import deepcopy
import psycopg2
import requests
from kubernetes_wrapper.logger import TangoLogger

LOG = TangoLogger.getLogger(__name__, log_level=logging.DEBUG, log_json=True)
TangoLogger.getLogger("k8s_wrapper:k8s-wrapper", logging.DEBUG, log_json=True)
LOG.setLevel(logging.DEBUG)

MAX_DEPLOYMENT_TIME = 60

class KubernetesWrapperEngine(object):

    def __init__(self, **kwargs):
        """
        Initialize cluster connection.
        :param app_id: string that identifies application

        """
        # trigger connection setup (without blocking)
        self.load_initial_config()
        # Threading workers
        self.thrd_pool = pool.ThreadPoolExecutor(max_workers=100)
        # Track the workers
        self.tasks = []

    def normalize(self, val):
        res = re.sub('[^A-Za-z0-9]+', '_',  val)
        # LOG.debug("Converted {} -> {}".format(val, res))
        return res

    def check_connection(self):
        url = 'http://www.google.com'
        timeout = 5
        try:
            _ = requests.get(url, timeout=timeout)
            return 'Always'
        except requests.ConnectionError:
            return 'IfNotPresent'

    def get_vim_config(self, vim_uuid):
        """
        Write in database the preparation of the service before instantiation
        This data will be used by ia-nbi to match service vim
        """
        connection = None
        try:
            connection = psycopg2.connect(user = os.getenv("POSTGRES_USER") or "sonata",
                                          password = os.getenv("POSTGRES_PASSWORD") or "sonatatest",
                                          host = os.getenv("DATABASE_HOST") or "son-postgres",
                                          port = os.getenv("DATABASE_PORT") or "5432",
                                          database = "vimregistry")

            cursor = connection.cursor()
            # Query
            cursor.execute("""
            SELECT configuration FROM VIM WHERE uuid = %s;
            """, 
            (vim_uuid,))
            configuration = cursor.fetchone()[0]
            connection.commit()
            configuration = yaml.safe_dump(configuration)
            return configuration

        except (Exception, psycopg2.Error) as error:
            if connection:
                connection.rollback()
            LOG.error("Error while connecting to PostgreSQL {}".format(error))

        finally:
            #closing database connection.
            if connection:
                cursor.close()
                connection.close()

    def get_vim_list(self):
        """
        Get the list of VIMs registered in the database
        """
        connection = None
        try:
            connection = psycopg2.connect(user = os.getenv("POSTGRES_USER") or "sonata",
                                          password = os.getenv("POSTGRES_PASSWORD") or "sonatatest",
                                          host = os.getenv("DATABASE_HOST") or "son-postgres",
                                          port = os.getenv("DATABASE_PORT") or "5432",
                                          database = "vimregistry")

            cursor = connection.cursor()
            # Query
            cursor.execute("""
            SELECT uuid FROM VIM WHERE vendor ='k8s';
            """, 
            )
            vim_list = cursor.fetchall()
            # Saving the results
            connection.commit()
            return vim_list

        except (Exception, psycopg2.Error) as error:
            if connection:
                connection.rollback()
            LOG.error("Error while connecting to PostgreSQL {}".format(error))

        finally:
            #closing database connection.
            if connection:
                cursor.close()
                connection.close()

    def load_initial_config(self):
        """
        Returns a kubernetes APIclient object for the initial configuration of the wrapper
        """
        vim_config = None
        vim_list = None
        while vim_list is not None:
            vim_list = self.get_vim_list()
            if vim_list is not None:
                if len(vim_list) > 0:
                    vim_list = str(list(vim_list[0])[0])
                    with open("/tmp/{}".format(vim_list), 'w') as f:
                        vim_config = self.get_vim_config(vim_list)
                        if vim_config is not None:
                            f.write(vim_config)
                    if vim_config is not None:
                        kube_config.load_kube_config(config_file="/tmp/{}".format(vim_list))
                else:
                    LOG.info("K8S Cluster is not configured")
            time.sleep(10) 

    def get_deployment_list(self, label, vim_uuid, namespace, watch=False, include_uninitialized=True, pretty='True' ):
        """
        CNF get deployment list method. This retrieve the list of deployments based on a label
        label: k8s deployment name
        namespace: Namespace where the deployment is deployed
        """
        t0 = time.time()
        deployment_name = None
        KubernetesWrapperEngine.get_vim_config(self, vim_uuid)
        k8s_beta = client.ExtensionsV1beta1Api()
        try: 
            deployment_name = k8s_beta.list_namespaced_deployment(namespace=namespace, label_selector=label)
        except ApiException as e:
            LOG.error("Exception when calling ExtensionsV1beta1Api->list_namespaced_deployment: %s\n" % e)
        if deployment_name.items:
            return deployment_name.items[0].metadata.name, deployment_name.items[0].spec.replicas
        else:
            return None, 0
        LOG.info("K8sGetDeploymentList-time: {} ms".format(int(round(time.time() - t0))* 1000))

    def create_patch_deployment(self, deployment_name, vim_uuid, deployment_namespace):
        t0 = time.time()
        KubernetesWrapperEngine.get_vim_config(self, vim_uuid)
        k8s_beta = client.ExtensionsV1beta1Api()
        patch = {"spec":{"template":{"metadata":{"annotations": {"updated_at": str(int(time.time()) * 1000)}}}}}
        try:
            patch = k8s_beta.patch_namespaced_deployment(name=deployment_name, namespace=deployment_namespace, 
                                                         body=patch, pretty='true')
        except ApiException as e:
            LOG.error("Exception when calling ExtensionsV1beta1Api->:patch_namespaced_deployment %s\n" % e)
        LOG.info("K8sCreatingPatch-time: {} ms".format(int(round(time.time() - t0))* 1000))
        return patch

    def create_configmap(self, config_map_id, instance_uuid, env_vars, service_uuid, vim_uuid, namespace = "default"):
        t0 = time.time()
        configuration = {}
        data = env_vars
        KubernetesWrapperEngine.get_vim_config(self, vim_uuid)
        k8s_beta = client.CoreV1Api()
        metadata = client.V1ObjectMeta(name = config_map_id, namespace = namespace,
                                       labels = {"instance_uuid": instance_uuid, 
                                                 "service_uuid": service_uuid,
                                                 "sp": "sonata"})
        configmap = None
        for x, y in data.items():
            x = KubernetesWrapperEngine.normalize(self, str(x))
            configuration[str(x)] = str(y)

        if isinstance(configuration, dict):
            body = client.V1ConfigMap(data = configuration, metadata = metadata)
            try:
                configmap = k8s_beta.create_namespaced_config_map(namespace, body)
            except ApiException as e:
                LOG.error("Exception when calling V1ConfigMap->create_namespaced_config_map: %s\n" % e)

        LOG.debug("Configmap: {}".format(configmap))
        LOG.info("K8sCreatingConfigmap-time: {} ms".format(int(round(time.time() - t0))* 1000))
        return configmap

    def overwrite_configmap(self, config_map_id, configmap, instance_uuid, env_vars, vim_uuid, namespace = "default"):
        t0 = time.time()
        KubernetesWrapperEngine.get_vim_config(self, vim_uuid)
        k8s_beta = client.CoreV1Api()
        for x, y in env_vars.items():
            x = KubernetesWrapperEngine.normalize(self, str(x))
            configmap.data.update({str(x): str(y)})
        body = configmap
        try:
            configmap_updated = k8s_beta.patch_namespaced_config_map(name = config_map_id, namespace = namespace, 
                                                                     body = body)
            LOG.debug("Configmap: {}".format(configmap_updated))
        except ApiException as e:
            LOG.error("Exception when calling V1ConfigMap->create_namespaced_config_map: %s\n" % e)
        LOG.info("K8sOverwriteConfigmap-time: {} ms".format(int(round(time.time() - t0))* 1000))
        return configmap_updated

    def scale_instance(self, deployment_name, replicas, vim_uuid, namespace, operation):
        """
        CNF scale in method. This remove one CNF from kubernetes deployment
        deployment: k8s deployment name
        namespace: Namespace where the deployment is deployed
        """
        t0 = time.time()
        service = {}
        ports = None
        KubernetesWrapperEngine.get_vim_config(self, vim_uuid)
        k8s_beta = client.ExtensionsV1beta1Api()
        client_k8s = client.CoreV1Api()
        if operation == "in":
            patch = {"spec":{"replicas": int(replicas) - 1}}
        elif operation == "out":
            patch = {"spec":{"replicas": int(replicas) + 1}}
        try:
            patch = k8s_beta.patch_namespaced_deployment_scale(name=deployment_name, namespace=namespace, 
                                                               body=patch, pretty='true')
        except ApiException as e:
            LOG.error("Exception when calling ExtensionsV1beta1Api->:patch_namespaced_deployment %s\n" % e)
        try:
            deployment = k8s_beta.read_namespaced_deployment(name=deployment_name, namespace=namespace, 
                                                             exact=False, export=False)
        except ApiException as e:
            LOG.error("Exception when calling ExtensionsV1beta1Api->read_namespaced_deployment: %s\n" % e)
        try:
            resp = client_k8s.read_namespaced_service(name=deployment_name, namespace=namespace, 
                                                            exact=False, export=False)
            service['message'] = "COMPLETED"
            service['ip_mapping'] = []

            loadbalancerip = resp.status.load_balancer.ingress[0].ip

            LOG.debug("loadbalancerip: {}".format(loadbalancerip))
            
            internal_ip = resp.spec.cluster_ip       
            ports = resp.spec.ports
            
            mapping = { "internal_ip": internal_ip, "floating_ip": loadbalancerip }
            service['ip_mapping'].append(mapping)
            if ports:
                service['ports'] = ports
            service['vnfr'] = resp
        except ApiException as e:
            LOG.error("Exception when calling ExtensionsV1beta1Api->read_namespaced_deployment: %s\n" % e)
        LOG.info("ScaleInstance-time: {} ms".format(int(round(time.time() - t0))* 1000))
        return deployment, service

    def get_deployment(self, deployment_name, vim_uuid, namespace, watch=False, include_uninitialized=True, pretty='True' ):
        """
        CNF get deployment method. This retrieve the deployment information object in kubernetes
        deployment: k8s deployment name
        namespace: Namespace where the deployment is deployed
        """
        t0 = time.time()
        deployment = None
        KubernetesWrapperEngine.get_vim_config(self, vim_uuid)
        k8s_beta = client.ExtensionsV1beta1Api()
        try:
            deployment = k8s_beta.read_namespaced_deployment(name=deployment_name, namespace=namespace, 
                                                             exact=False, export=False)
            LOG.debug("Deployment: {}".format(deployment))
        except ApiException as e:
            LOG.error("Exception when calling ExtensionsV1beta1Api->read_namespaced_deployment: %s\n" % e)
        LOG.info("K8sGetDeployment-time: {} ms".format(int(round(time.time() - t0))* 1000))
        return deployment

    def get_configmap(self, config_map_id, vim_uuid, namespace, watch=False, include_uninitialized=True, pretty='True' ):
        """
        CNF get configmap method. This retrieve the configmap information object in kubernetes
        config_map_id: k8s config map id
        namespace: Namespace where the deployment is deployed
        """
        t0 = time.time()
        configmap = None
        KubernetesWrapperEngine.get_vim_config(self, vim_uuid)
        k8s_beta = client.CoreV1Api()
        try:
            configmap = k8s_beta.read_namespaced_config_map(name = config_map_id, namespace = namespace, 
                                                            exact=False, export=False)
            LOG.debug("Configmap: {}".format(configmap))
        except ApiException as e:
            LOG.error("Exception when calling ExtensionsV1beta1Api->read_namespaced_deployment: %s\n" % e)
        LOG.info("K8sGetConfigmap-time: {} ms".format(int(round(time.time() - t0))* 1000))
        return configmap

    def create_deployment(self, deployment, vim_uuid, namespace, watch=False, include_uninitialized=True, pretty='True' ):
        """
        CNF Instantiation method. This schedule a deployment object in kubernetes
        deployment: k8s deployment object
        namespace: Namespace where the deployment will be deployed
        """
        t0 = time.time()
        reply = {}
        status = None
        message = None
        KubernetesWrapperEngine.get_vim_config(self, vim_uuid)
        k8s_beta = client.ExtensionsV1beta1Api()
        resp = k8s_beta.create_namespaced_deployment(
                        body=deployment, namespace=namespace , async_req=False)

        for iteration in range(MAX_DEPLOYMENT_TIME):
            if iteration == MAX_DEPLOYMENT_TIME - 1:
                status = "ERROR"
                message = "Deployment time exceeded"
                break
            elif status == "COMPLETED":
                break

            try: 
                resp = k8s_beta.read_namespaced_deployment(resp.metadata.name, namespace=namespace, exact=False, 
                                                           export=False)
                if resp.status.conditions:
                    conditions = resp.status.conditions
                    if len(conditions) >=  1:
                        for condition in conditions:
                            if condition.status:
                                LOG.debug("Deployment created. Status={} Message={} Reason={}".format(condition.status, 
                                                                                                      condition.message, 
                                                                                                      condition.reason))
                                if condition.status == "False":
                                    status = "ERROR"
                                    break
                                elif condition.status == "True":
                                    if condition.reason == "ReplicaSetUpdated":
                                        pass
                                    elif condition.reason == "NewReplicaSetAvailable" or "MinimumReplicasAvailable":
                                        status = "COMPLETED"
                                        message = None
                                        break
                                break

            except ApiException as e:
                LOG.error("Exception when calling ExtensionsV1beta1Api->read_namespaced_deployment: %s\n" % e)
                reply['message'] = e
                reply['instanceName'] = str(resp.metadata.name)
                reply['instanceVimUuid'] = "unknown"
                reply['vimUuid'] = vim_uuid
                reply['request_status'] = "ERROR"
                reply['ip_mapping'] = None
                reply['vnfr'] = resp
                return reply
            time.sleep(2)
        
        reply['message'] = message
        reply['instanceName'] = str(resp.metadata.name)
        reply['instanceVimUuid'] = "unknown"
        reply['vimUuid'] = vim_uuid
        reply['request_status'] = status
        reply['ip_mapping'] = None
        reply['vnfr'] = resp.to_dict
        LOG.info("K8sCreatingDeployment-time: {} ms".format(int(round(time.time() - t0))* 1000))
        return reply

    def create_service(self, service, vim_uuid, namespace, watch=False, include_uninitialized=True, pretty='True' ):
        """
        CNF Instantiation method. This schedule a service object in kubernetes that provide networking to a deployment
        service: k8s service object
        namespace: Namespace where the service will be deployed
        """
        t0 = time.time()
        reply = {}
        status = None
        message = None
        KubernetesWrapperEngine.get_vim_config(self, vim_uuid)
        k8s_beta = client.CoreV1Api()
        resp = k8s_beta.create_namespaced_service(
                        body=service, namespace=namespace , async_req=False)
        
        res_name = resp.metadata.name

        for iteration in range(MAX_DEPLOYMENT_TIME):
            if iteration == MAX_DEPLOYMENT_TIME - 1:
                status = "ERROR"
                message = "Deployment time exceeded"
                break
            try:    
                resp2 = k8s_beta.read_namespaced_service_status(name=res_name , namespace=namespace , async_req=False)
                if resp2.status.load_balancer.ingress is not None:
                    break
            except ApiException as e:
                LOG.error("Exception when calling ExtensionsV1beta1Api->read_namespaced_deployment: %s\n" % e)
            time.sleep(2)

        message = "COMPLETED"
        reply['message'] = message
        reply['instanceName'] = str(resp.metadata.name)
        reply['request_status'] = status
        reply['ip_mapping'] = []

        loadbalancerip = resp2.status.load_balancer.ingress[0].ip

        LOG.debug("loadbalancerip: {}".format(loadbalancerip))
        
        internal_ip = resp2.spec.cluster_ip       
        ports = resp2.spec.ports
        
        mapping = { "internal_ip": internal_ip, "floating_ip": loadbalancerip }
        reply['ip_mapping'].append(mapping)
        reply['ports'] = None
        if status:
            reply['ports'] = ports
        reply['vnfr'] = resp2
        LOG.info("K8sCreatingService-time: {} ms".format(int(round(time.time() - t0))* 1000))
        return reply
   
    def remove_service(self, service_uuid, namespace, vim_uuid, watch=False, include_uninitialized=True, pretty='True'):
        """
        CNF remove method. This remove a service
        service: k8s service object
        namespace: Namespace where the service is deployed
        """
        t0 = time.time()
        reply = {}
        status = None
        message = None
        KubernetesWrapperEngine.get_vim_config(self, vim_uuid)
        k8s_beta = client.ExtensionsV1beta1Api()

        LOG.debug("Deleting deployments")
        # Delete deployment
        try: 
            resp = k8s_beta.list_namespaced_deployment(namespace, label_selector="service_uuid={}".format(service_uuid))
        except ApiException as e:
            LOG.error("Exception when calling ExtensionsV1beta1Api->list_namespaced_deployment: {}".format(e))
            status = False
            message = str(e)

        k8s_deployments = []
        for k8s_deployment_list in resp.items:
            k8s_deployments.append(k8s_deployment_list.metadata.name)

        LOG.debug("k8s deployment list {}".format(k8s_deployments))
        body = client.V1DeleteOptions(propagation_policy="Foreground", grace_period_seconds=5)
        for k8s_deployment in k8s_deployments:
            LOG.debug("k8s_deployment: " + str(k8s_deployment))
            LOG.debug("namespace: " + str(namespace))
            LOG.debug("body: " + str(body))
            try:
                resp = k8s_beta.delete_namespaced_deployment(k8s_deployment, namespace, body=body)
            except ApiException as e:
                LOG.error("Exception when calling ExtensionsV1beta1Api->delete_namespaced_deployment: {}".format(e))
                status = False
                message = str(e)

        LOG.debug("Deleting services")
        # Delete services
        k8s_beta = client.CoreV1Api()
        try: 
            resp = k8s_beta.list_namespaced_service(namespace, label_selector="service_uuid={}".format(service_uuid))
        except ApiException as e:
            LOG.error("Exception when calling CoreV1Api->list_namespaced_services: {}".format(e))
            status = False
            message = str(e)

        k8s_services = []
        for k8s_service_list in resp.items:
            k8s_services.append(k8s_service_list.metadata.name)

        LOG.debug("k8s service list {}".format(k8s_services))
        k8s_beta = client.CoreV1Api()
        body = client.V1DeleteOptions(propagation_policy="Foreground", grace_period_seconds=5)
        for k8s_service in k8s_services:
            LOG.debug("k8s_service: " + str(k8s_service))
            LOG.debug("namespace: " + str(namespace))
            LOG.debug("body: " + str(body))            
            try: 
                resp = k8s_beta.delete_namespaced_service(k8s_service, namespace, body=body)
            except ApiException as e:
                LOG.error("Exception when calling CoreV1Api->delete_namespaced_service: {}".format(e))
                status = False
                message = str(e)                

        LOG.debug("Deleting configmaps")
        # Delete configmaps
        k8s_beta = client.CoreV1Api()
        try: 
            resp = k8s_beta.delete_collection_namespaced_config_map(namespace, label_selector=
                                                                    "service_uuid={}".format(service_uuid))
        except ApiException as e:
            LOG.error("Exception when calling CoreV1Api->delete_collection_namespaced_config_map: {}".format(e))
            status = False
            message = str(e)

        LOG.info("MainRemoveService-time: {} ms".format(int(round(time.time() - t0))* 1000))
        return message
    
    def deployment_object(self, instance_uuid, cnf_yaml, service_uuid, vim_uuid):
        """
        CNF modeling method. This build a deployment object in kubernetes
        instance_uuid: k8s deployment name
        cnf_yaml: CNF Descriptor in yaml format
        """
        t0 = time.time()
        LOG.debug("CNFD: {}".format(cnf_yaml))
        container_list = []
        deployment_k8s = None
        if "cloudnative_deployment_units" in cnf_yaml:
            cdu = cnf_yaml.get('cloudnative_deployment_units')
            for cdu_obj in cdu:
                env_vars = env_from = cpu = memory = huge_pages = gpu = sr_iov = resources = None
                port_list = []
                environment = []
                cdu_id = cdu_obj.get('id')
                image = cdu_obj.get('image')
                cdu_conex = cdu_obj.get('connection_points')
                container_name = cdu_id
                config_map_id = cdu_id
                if cdu_obj.get('parameters'):
                    env_vars = cdu_obj['parameters'].get('env')
                if cdu_obj.get('resource_requirements'):
                    gpu = cdu_obj['resource_requirements'].get('gpu')
                    cpu = cdu_obj['resource_requirements'].get('cpu')
                    memory = cdu_obj['resource_requirements'].get('memory')
                    sr_iov = cdu_obj['resource_requirements'].get('sr-iov')
                    huge_pages = cdu_obj['resource_requirements'].get('huge-pages')
                if cdu_conex:
                    for po in cdu_conex:
                        port = po.get('port')
                        port_name = po.get('id')
                        port_list.append(client.V1ContainerPort(container_port = port, name = port_name))

                limits = {}
                requests = {}
                if gpu:
                    LOG.debug("Features requested: {}".format(gpu))
                    # gpu_type can be amd or nvidia
                    for gpu_type, amount in gpu.items():
                        limits["{}.com/gpu".format(gpu_type)] = amount
                if cpu:
                    # TODO
                    pass
                if memory:
                    # TODO
                    pass               
                if sr_iov:
                    # TODO
                    pass                  
                if huge_pages:
                    # TODO
                    pass  
                
                resources = client.V1ResourceRequirements(limits=limits, requests=requests)             

                # Environment variables from descriptor
                if env_vars:
                    LOG.debug("Configmap: {}".format(config_map_id))
                    KubernetesWrapperEngine.create_configmap(self, config_map_id, instance_uuid, env_vars, service_uuid,
                                                             vim_uuid, namespace = "default")
                else:
                    env_vars = {"sonata": "rules"}
                    LOG.debug("Configmap: {}".format(config_map_id))
                    KubernetesWrapperEngine.create_configmap(self, config_map_id, instance_uuid, env_vars, service_uuid, 
                                                             vim_uuid, namespace = "default")
                env_from = client.V1EnvFromSource(config_map_ref = client.V1ConfigMapEnvSource(name = config_map_id, 
                                                  optional = False))

                # Default static environment variables
                environment.append(client.V1EnvVar(name="instance_uuid", value=instance_uuid))
                environment.append(client.V1EnvVar(name="service_uuid", value=service_uuid))
                environment.append(client.V1EnvVar(name="container_name", value=container_name))
                environment.append(client.V1EnvVar(name="vendor", value=KubernetesWrapperEngine.normalize(self, cnf_yaml.get('vendor'))))
                environment.append(client.V1EnvVar(name="name", value=KubernetesWrapperEngine.normalize(self, cnf_yaml.get('name'))))
                environment.append(client.V1EnvVar(name="version", value=KubernetesWrapperEngine.normalize(self, cnf_yaml.get('version'))))

                image_pull_policy = KubernetesWrapperEngine.check_connection(self)

                # Configureate Pod template cont ainer
                container = client.V1Container(
                    env = environment,
                    name = container_name,
                    resources = resources,
                    image = image,
                    image_pull_policy = image_pull_policy,
                    ports = port_list,
                    env_from = [env_from])
                container_list.append(container)
        else:
            return deployment_k8s
        
        # Create and configurate a spec section
        deployment_label =  ("{}-{}-{}-{}".format(cnf_yaml.get("vendor"), cnf_yaml.get("name"), cnf_yaml.get("version"),
                             instance_uuid.split("-")[0])).replace(".", "-")
        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={'deployment': deployment_label,
                                                 'instance_uuid': cnf_yaml['instance_uuid'],
                                                 'service_uuid': service_uuid,
                                                 'sp': "sonata",
                                                 'descriptor_uuid': cnf_yaml['uuid']} 
                                                 ),
            spec=client.V1PodSpec(containers=container_list))
        # Create the specification of deployment
        spec = client.ExtensionsV1beta1DeploymentSpec(
            replicas=1,
            template=template)
        # Instantiate the deployment object
        deployment_k8s = client.ExtensionsV1beta1Deployment(
            api_version="extensions/v1beta1",
            kind="Deployment",
            metadata=client.V1ObjectMeta(name=deployment_label),
            spec=spec)
        LOG.info("CreatingDeploymentObject-time: {} ms".format(int(round(time.time() - t0))* 1000))
        return deployment_k8s

    def service_object(self, instance_uuid, cnf_yaml, deployment_selector, service_uuid):
        """
        CNF modeling method. This build a service object in kubernetes
        instance_uuid: function uuid
        cnf_yaml: CNF Descriptor in yaml format
        deployment_selector: The deployment where the service will forward the traffic
        """
        t0 = time.time()
        ports_services=[]
        if cnf_yaml.get("connection_points"):
            for connection_points in cnf_yaml["connection_points"]:
                port_id = connection_points["id"]
                port_number = connection_points["port"]
                if cnf_yaml.get("virtual_links"):
                    for vl in cnf_yaml["virtual_links"]:
                        vl_cp = vl["connection_points_reference"]
                        if port_id in vl_cp:
                            for cdu in cnf_yaml["cloudnative_deployment_units"]:
                                for cpr in vl_cp:
                                    if ":" in cpr:
                                        cpr_cdu = cpr.split(":")[0]
                                        cpr_cpid = cpr.split(":")[1]
                                        if cpr_cdu == cdu["id"].split("-")[0]:
                                            for cdu_cp in cdu["connection_points"]:
                                                if cpr_cpid == cdu_cp["id"]:
                                                        port_service = {}
                                                        port_service["name"] = port_id
                                                        port_service["port"] = port_number
                                                        port_service["target_port"] = cdu_cp["port"]
                                                        ports_services.append(port_service)               
        
        # Create the specification of service
        spec = client.V1ServiceSpec(
            ports=ports_services,
            selector={'deployment': deployment_selector},
            type="LoadBalancer")
        # Instantiate the deployment object
        service = client.V1Service(
            api_version="v1",
            kind="Service",
            metadata=client.V1ObjectMeta(name=deployment_selector, 
                                         namespace="default", 
                                         labels = {"service_uuid": service_uuid,
                                                   "sp": "sonata"}),
            spec=spec)
        LOG.info("CreatingServiceObject-time: {} ms".format(int(round(time.time() - t0))* 1000))
        return service

    def resource_object(self, vim_uuid):
        t0 = time.time()
        LOG.debug("vim uuid: {}").format(vim_uuid)
        KubernetesWrapperEngine.get_vim_config(self, vim_uuid)
        api = client.CoreV1Api()
        nodes = api.list_node().to_dict()
        resources = []
        if nodes.get("items"):
            for node in nodes["items"]:
                # LOG.debug("STATUS: {}".format(node["status"]))
                resource = {}
                resource["node_name"] = node["metadata"].get("name")
                # pods = api.list_namespaced_pod(namespace="default", field_selector="spec.nodeName={}".format(resource["node_name"])).to_dict()
                # amd_gpu = 0
                # nvidia_gpu = 0
                # LOG.debug("PODS.ITEMS: {}".format(dict(pods.items)))
                # for pod in dict(pods.items):
                #     LOG.debug("pod: {}".format(pod))
                #     for container in pod.spec.containers:
                #         if container.resources:
                #             LOG.debug("container.resources.limits: {}".format(container.resources.limits))
                #             limits = container.resource.limits
                #             if "amd.com/gpu" in limits:                        
                #                 amd_gpu += 1
                #             if "nvidia.com/gpu" in limits:
                #                 nvidia_gpu += 1
                resource["core_total"] = node["status"]["capacity"].get("cpu")
                resource["memory_total"] = node["status"]["capacity"].get("memory")
                resource["amd_gpu_total"] = node["status"]["capacity"].get("amd.com/gpu", 0)
                resource["nvidia_gpu_total"] = node["status"]["capacity"].get("nvidia.com/gpu", 0)
                resource["memory_allocatable"] = node["status"]["allocatable"].get("memory")
                resource["amd_gpu_allocatable"] = node["status"]["allocatable"].get("amd.com/gpu", 0)
                # resource["amd_gpu_allocatable"] = amd_gpu
                resource["nvidia_gpu_allocatable"] = node["status"]["allocatable"].get("nvidia.com/gpu", 0)
                # resource["nvidia_gpu_allocatable"] = nvidia_gpu
                resources.append(resource)
        # Response:
        # { resources: [{ node-name: k8s, core_total: 16, memory_total: 32724804, memory_allocatable: 32724804}] }
        LOG.info("CreatingResourcesObject-time: {} ms".format(int(round(time.time() - t0))* 1000))
        return resources

    def node_metrics_object(self, vim_uuid):
        """
        Monitoring metrics from cluster
        vim_uuid: cluster if to get the metrics
        """
        t0 = time.time()
        cpu_used = 0
        memory_used = 0 
        KubernetesWrapperEngine.get_vim_config(self, vim_uuid)
        api = client.ApiClient()
        LOG.info(str(api))
        try:
            response = api.call_api('/apis/metrics.k8s.io/v1beta1/nodes', 'GET', _return_http_data_only=True, response_type=str)
            jsonify_response = response.replace("'","\"")
            json_response = json.loads(jsonify_response)     
            if json_response.get("items"):
                for item in json_response["items"]:
                    if item.get("usage"):
                        cpu = item["usage"]["cpu"]
                        memory = item["usage"]["memory"]
                        cpu_used += int(cpu[0:-1])
                        memory_used += int(memory[0:-2])   
            LOG.debug("CPU Used: {} Memory Used: {}".format(cpu_used, memory_used))
        except ApiException as e:
            LOG.error("Exception when calling /apis/metrics.k8s.io/v1beta1/nodes: GET {}".format(e))
        LOG.info("MonitoringMetrics-time: {} ms".format(int(round(time.time() - t0))* 1000))
        return (cpu_used, memory_used)

test = KubernetesWrapperEngine()