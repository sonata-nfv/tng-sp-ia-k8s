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
from os import path
import yaml, json
import uuid
from pprint import pprint
from copy import deepcopy
import psycopg2

logging.basicConfig(level=logging.INFO)
logging.getLogger('pika').setLevel(logging.ERROR)
LOG = logging.getLogger("k8s-wrapper:k8s-wrapper")
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
        # LOG.debug("config: " + str(config) )
        # Threading workers
        self.thrd_pool = pool.ThreadPoolExecutor(max_workers=100)
        # Track the workers
        self.tasks = []

    def get_vim_config(self, vim_uuid):
        """
        Write in database the preparation of the service before instantiation
        This data will be used by ia-nbi to match service vim
        """

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
            # LOG.debug("Configuration:" + str(configuration))
            # Saving the results
            connection.commit()
            # LOG.info("Configuration DB: " + str(configuration))
            configuration = yaml.safe_dump(configuration)
            # LOG.info("Configuration DB yaml: " + str(configuration))
            return configuration

        except (Exception, psycopg2.Error) as error:
            if connection:
                connection.rollback()
            LOG.error("Error while connecting to PostgreSQL " + str(error))

        finally:
            #closing database connection.
            if connection:
                cursor.close()
                connection.close()
                LOG.info("PostgreSQL connection is closed")

    def get_vim_list(self):
        """
        Get the list of VIMs registered in the database
        """

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
            # LOG.info("VIM_LIST: " + str(vim_list))
            return vim_list

        except (Exception, psycopg2.Error) as error:
            if connection:
                connection.rollback()
            LOG.error("Error while connecting to PostgreSQL " + str(error))

        finally:
            #closing database connection.
            if connection:
                cursor.close()
                connection.close()
                LOG.info("PostgreSQL connection is closed")

    def load_initial_config(self):
        """
        Returns a kubernetes APIclient object for the initial configuration of the wrapper
        """
        vim_config = None
        vim_list = None
        if os.getenv("KUBECONFIG") is True:
            kube_config.load_kube_config()
        else:
            vim_list = self.get_vim_list()
            if vim_list is not None:
                if len(vim_list) > 0:
                    vim_list = str(list(vim_list[0])[0])
                    with open("/tmp/" + vim_list, 'w') as f:
                        vim_config = self.get_vim_config(vim_list)
                        if vim_config is not None:
                            f.write(vim_config)
                    if vim_config is not None:
                        kube_config.load_kube_config(config_file="/tmp/" + vim_list)
                else:
                    LOG.info("K8S Cluster is not configured")

    def create_deployment(self, deployment, namespace, watch=False, include_uninitialized=True, pretty='True' ):
        """
        CNF Instantiation method. This schedule a deployment object in kubernetes
        deployment: k8s deployment object
        namespace: Namespace where the deployment will be deployed
        """
        reply = {}
        status = None
        message = None
        k8s_beta = client.ExtensionsV1beta1Api()
        resp = k8s_beta.create_namespaced_deployment(
                        body=deployment, namespace=namespace , async_req=False)

        for iteration in range(MAX_DEPLOYMENT_TIME):
            # LOG.info(iteration)
            if iteration == MAX_DEPLOYMENT_TIME - 1:
                status = "ERROR"
                message = "Deployment time exceeded"
                break
            elif status == "COMPLETED":
                break

            try: 
                resp = k8s_beta.read_namespaced_deployment(resp.metadata.name, namespace=namespace, exact=False, export=False)
                if resp.status.conditions:
                    conditions = resp.status.conditions
                    if len(conditions) >=  1:
                        for condition in conditions:
                            if condition.status:
                                # LOG.info(str(condition))
                                LOG.info("Deployment created. Status='%s' Message='%s' Reason='%s'" % 
                                        (str(condition.status), 
                                        str(condition.message), 
                                        str(condition.reason)))
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
                LOG.info("Exception when calling ExtensionsV1beta1Api->read_namespaced_deployment: %s\n" % e)
                reply['message'] = e
                reply['instanceName'] = str(resp.metadata.name)
                reply['instanceVimUuid'] = "unknown"
                reply['vimUuid'] = "unknown"
                reply['request_status'] = "ERROR"
                reply['ip_mapping'] = None
                reply['vnfr'] = resp
                return reply
            time.sleep(2)
        
        reply['message'] = message
        reply['instanceName'] = str(resp.metadata.name)
        reply['instanceVimUuid'] = "unknown"
        reply['vimUuid'] = "unknown" 
        reply['request_status'] = status
        reply['ip_mapping'] = None
        reply['vnfr'] = resp.to_dict
        return reply

    def create_service(self, service, namespace, watch=False, include_uninitialized=True, pretty='True' ):
        """
        CNF Instantiation method. This schedule a service object in kubernetes that provide networking to a deployment
        service: k8s service object
        namespace: Namespace where the service will be deployed
        """
        reply = {}
        status = None
        message = None
        k8s_beta = client.CoreV1Api()
        resp = k8s_beta.create_namespaced_service(
                        body=service, namespace=namespace , async_req=False)
        
        res_name = resp.metadata.name

        for iteration in range(MAX_DEPLOYMENT_TIME):
            # LOG.info(iteration)
            if iteration == MAX_DEPLOYMENT_TIME - 1:
                status = "ERROR"
                message = "Deployment time exceeded"
                break
            try:    
                resp2 = k8s_beta.read_namespaced_service_status(name=res_name , namespace=namespace , async_req=False)
                if resp2.status.load_balancer.ingress is not None:
                    break
            except ApiException as e:
                LOG.info("Exception when calling ExtensionsV1beta1Api->read_namespaced_deployment: %s\n" % e)
            time.sleep(2)

        # LOG.info("READING SERVICE STATUS: " + str(resp2))

        message = "COMPLETED"
        reply['message'] = message
        reply['instanceName'] = str(resp.metadata.name)
        reply['request_status'] = status
        reply['ip_mapping'] = []

        loadbalancerip = resp2.status.load_balancer.ingress[0].ip

        LOG.info("loadbalancerip: " + str(loadbalancerip))
        
        internal_ip = resp2.spec.cluster_ip
        # LOG.info("SPEC: " + str(internal_ip))
        
        ports = resp2.spec.ports
        
        mapping = { "internal_ip": internal_ip, "floating_ip": loadbalancerip }
        reply['ip_mapping'].append(mapping)
        reply['ports'] = None
        if status:
            reply['ports'] = ports
        reply['vnfr'] = resp2
        return reply
   
    def deployment_object(self, DEPLOYMENT_NAME, cnf_yaml):
        """
        CNF modeling method. This build a deployment object in kubernetes
        DEPLOYMENT_NAME: k8s deployment name
        cnf_yaml: CNF Descriptor in yaml format
        """
        # LOG.info("CNFD: " + str(cnf_yaml))
        port_list = []
        if "cloudnative_deployment_units" in cnf_yaml:
            cdu = cnf_yaml.get('cloudnative_deployment_units')
            for cdu_obj in cdu:
                cdu_id = cdu_obj.get('id')
                cdu_image = cdu_obj.get('image')
                cdu_conex = cdu_obj.get('connection_points')
                container_name = cdu_id
                image = cdu_image
                if cdu_conex:
                    for po in cdu_conex:
                        port = po.get('port')
                        port_name = po.get('id')
                        port_list.append(client.V1ContainerPort(container_port=port, name=port_name))
        else:
            pass

        # LOG.info("Result: " + str(container_name) + str(image) + str(port))
        
        # Configureate Pod template container
        container = client.V1Container(
            name=container_name,
            image=image,
            ports=port_list)
        # Create and configurate a spec section
        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={'deployment': container_name}),
            spec=client.V1PodSpec(containers=[container]))
        # Create the specification of deployment
        spec = client.ExtensionsV1beta1DeploymentSpec(
            replicas=1,
            template=template)
        # Instantiate the deployment object
        deployment_k8s = client.ExtensionsV1beta1Deployment(
            api_version="extensions/v1beta1",
            kind="Deployment",
            metadata=client.V1ObjectMeta(name=DEPLOYMENT_NAME),
            spec=spec)
        return deployment_k8s

    def service_object(self, DEPLOYMENT_NAME, cnf_yaml, deployment_selector):
        """
        CNF modeling method. This build a service object in kubernetes
        DEPLOYMENT_NAME: k8s deployment name
        cnf_yaml: CNF Descriptor in yaml format
        deployment_selector: The deployment where the service will forward the traffic
        """
        # LOG.info("CNFD: " + str(cnf_yaml))
        ports_services=[]
        if cnf_yaml.get("connection_points"):
            for connection_points in cnf_yaml["connection_points"]:
                # LOG.info("CONNECTION_POINTS:" + str(connection_points))
                port_id = connection_points["id"]
                port_number = connection_points["port"]
                # LOG.info("port_id: " + str(port_id) + " port_number: " + str(port_number))
                if cnf_yaml.get("virtual_links"):
                    for vl in cnf_yaml["virtual_links"]:
                        vl_cp = vl["connection_points_reference"]
                        # LOG.info("VLS_CP: " + str(vl_cp))
                        if port_id in vl_cp:
                            # LOG.info("Found cp in vl_cp")
                            for cdu in cnf_yaml["cloudnative_deployment_units"]:
                                # LOG.info("loop cdus")
                                # LOG.debug("vl_cp: " + str(vl_cp))
                                for cpr in vl_cp:
                                    # LOG.info("cpr: " + str(cpr))
                                    if ":" in cpr:
                                        cpr_cdu = cpr.split(":")[0]
                                        cpr_cpid = cpr.split(":")[1]
                                        # LOG.info("Comparison cpr_cdu == cdu[id]:" + str(cpr_cdu) + " " + str(cdu["id"].split("-")[0]))
                                        if cpr_cdu == cdu["id"].split("-")[0]:
                                            for cdu_cp in cdu["connection_points"]:
                                                if cpr_cpid == cdu_cp["id"]:
                                                        port_service = {}
                                                        port_service["name"] = port_id
                                                        port_service["port"] = port_number
                                                        port_service["target_port"] = cdu_cp["port"]
                                                        ports_services.append(port_service)               
        # LOG.info("Port_Services: " + str(ports_services))
        # LOG.info("Deployment Selector:" + str(deployment_selector).replace("\n", ""))
        
        # Create the specification of service
        spec = client.V1ServiceSpec(
            ports=ports_services,
            selector={
                'deployment': deployment_selector
                },
            type="LoadBalancer")
        # Instantiate the deployment object
        service = client.V1Service(
            api_version="v1",
            kind="Service",
            metadata=client.V1ObjectMeta(name="service-" + DEPLOYMENT_NAME, namespace="default"),
            spec=spec)
        # LOG.info(service)
        return service

    def resource_object(self, vim_uuid):
        KubernetesWrapperEngine.get_vim_config(self, vim_uuid)
        api = client.CoreV1Api()

        nodes = api.list_node().to_dict()
        # LOG.info(nodes)
        resources = []
        
        if nodes.get("items"):
            for node in nodes["items"]:
                resource = {}
                resource["node_name"] = node["metadata"].get("name")
                resource["core_total"] = node["status"]["capacity"].get("cpu")
                resource["memory_total"] = node["status"]["capacity"].get("memory")
                resource["memory_allocatable"] = node["status"]["allocatable"].get("memory")
                # LOG.info(resource)
                resources.append(resource)
        # Response:
        # { resources: [{ node-name: k8s, core_total: 16, memory_total: 32724804, memory_allocatable: 32724804}] }

        return resources

    def node_metrics_object(self, vim_uuid):
        """
        Monitoring metrics from cluster
        vim_uuid: cluster if to get the metrics
        """

        KubernetesWrapperEngine.get_vim_config(self, vim_uuid)
        api = client.ApiClient()
        response = api.call_api('/apis/metrics.k8s.io/v1beta1/nodes', 'GET', _return_http_data_only=True, response_type=str)
        jsonify_response = response.replace("'","\"")
        json_response = json.loads(jsonify_response)
        cpu_used = 0
        memory_used = 0        
        if json_response.get("items"):
            for item in json_response["items"]:
                if item.get("usage"):
                    cpu = item["usage"]["cpu"]
                    memory = item["usage"]["memory"]
                    cpu_used += int(cpu[0:-1])
                    memory_used += int(memory[0:-2])   
        # LOG.info("CPU Used: " + str(cpu_used) + "Memory Used:" + str(memory_used))
        return (cpu_used, memory_used)

test = KubernetesWrapperEngine()