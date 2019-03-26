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

    def get_deployment_list(self, label, namespace, watch=False, include_uninitialized=True, pretty='True' ):
        """
        CNF get deployment list method. This retrieve the list of deployments based on a label
        label: k8s deployment name
        namespace: Namespace where the deployment is deployed
        """
        deployment_name = None
        k8s_beta = client.ExtensionsV1beta1Api()
        try: 
            deployment_name = k8s_beta.list_namespaced_deployment(namespace=namespace, label_selector=label)
        except ApiException as e:
            LOG.error("Exception when calling ExtensionsV1beta1Api->list_namespaced_deployment: %s\n" % e)
        # LOG.info(str(deployment_name).replace("'","\"").replace(" ","").replace("\n",""))
        # LOG.info("METADATA_NAME: " + str(deployment_name.items[0].metadata.name))
        return deployment_name.items[0].metadata.name

    def create_patch_deployment(self, deployment_name, deployment_namespace):
        k8s_beta = client.ExtensionsV1beta1Api()
        patch = {"spec":{"template":{"metadata":{"annotations": {"updated_at": str(int(time.time()) * 1000)}}}}}
        try:
            patch = k8s_beta.patch_namespaced_deployment(name=deployment_name, namespace=deployment_namespace, body=patch, pretty='true')
        except ApiException as e:
            LOG.error("Exception when calling ExtensionsV1beta1Api->:patch_namespaced_deployment %s\n" % e)
        return patch

    def create_configmap(self, config_map_id, instance_uuid, env_vars, service_uuid, namespace = "default"):
        configmap_updated = None
        configuration = {}
        data = env_vars
        LOG.info("Vars received: " + str(env_vars).replace("'","\"").replace(" ","").replace("\n",""))
        k8s_beta = client.CoreV1Api()
        metadata = client.V1ObjectMeta(name = config_map_id, namespace = namespace,
                                       labels = {"instance_uuid": instance_uuid, "service_uuid": service_uuid})
        configmap = None
        for x, y in data.items():
            configuration[str(x)] = str(y)
        LOG.info("Data dict: " + str(configuration).replace("'","\"").replace(" ","").replace("\n",""))

        if isinstance(configuration, dict):
            body = client.V1ConfigMap(data = configuration, metadata = metadata)
            try:
                configmap = k8s_beta.create_namespaced_config_map(namespace, body)
            except ApiException as e:
                LOG.error("Exception when calling V1ConfigMap->create_namespaced_config_map: %s\n" % e)

        LOG.info("Configmap:" + str(configmap).replace("'","\"").replace(" ","").replace("\n",""))
        return configmap

    def overwrite_configmap(self, config_map_id, configmap, instance_uuid, env_vars, namespace = "default"):
        LOG.info("Vars received: " + str(env_vars).replace("'","\"").replace(" ","").replace("\n",""))
        LOG.info("config map: " + str(configmap).replace("'","\"").replace(" ","").replace("\n",""))

        k8s_beta = client.CoreV1Api()
        for x, y in env_vars.items():
            LOG.info("Name: " + str(x))
            LOG.info("value: " + str(y))
            LOG.info("data: " + str(configmap).replace("'","\"").replace(" ","").replace("\n",""))
            configmap.data.update({str(x): str(y)})
        LOG.info("Data dict: " + str(configmap).replace("'","\"").replace(" ","").replace("\n",""))
        body = configmap
        try:
            configmap_updated = k8s_beta.patch_namespaced_config_map(name = config_map_id, namespace = namespace, body = body)
            LOG.info("Configmap:" + str(configmap_updated))
        except ApiException as e:
            LOG.error("Exception when calling V1ConfigMap->create_namespaced_config_map: %s\n" % e)
        return configmap_updated

    def get_deployment(self, deployment_name, namespace, watch=False, include_uninitialized=True, pretty='True' ):
        """
        CNF get deployment method. This retrieve the deployment information object in kubernetes
        deployment: k8s deployment name
        namespace: Namespace where the deployment is deployed
        """
        deployment = None
        k8s_beta = client.ExtensionsV1beta1Api()
        try:
            deployment = k8s_beta.read_namespaced_deployment(name=deployment_name, namespace=namespace, exact=False, export=False)
        except ApiException as e:
            LOG.error("Exception when calling ExtensionsV1beta1Api->read_namespaced_deployment: %s\n" % e)
        # LOG.info(str(deployment))
        return deployment

    def get_configmap(self, config_map_id, namespace, watch=False, include_uninitialized=True, pretty='True' ):
        """
        CNF get configmap method. This retrieve the configmap information object in kubernetes
        config_map_id: k8s config map id
        namespace: Namespace where the deployment is deployed
        """
        configmap = None
        k8s_beta = client.CoreV1Api()
        try:
            configmap = k8s_beta.read_namespaced_config_map(name = config_map_id, namespace = namespace, exact=False, export=False)
        except ApiException as e:
            LOG.error("Exception when calling ExtensionsV1beta1Api->read_namespaced_deployment: %s\n" % e)
        # LOG.info(str(deployment))
        return configmap

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
   
    def remove_service(self, service_uuid, namespace, vim_uuid, watch=False, include_uninitialized=True, pretty='True' ):
        """
        CNF remove method. This remove a service
        service: k8s service object
        namespace: Namespace where the service will be deployed
        """
        reply = {}
        status = None
        message = None
        k8s_beta = client.ExtensionsV1beta1Api()

        LOG.info("Deleting deployments")
        # Delete deployment
        try: 
            resp = k8s_beta.delete_collection_namespaced_deployment(namespace, label_selector="service_uuid=" + service_uuid)
            LOG.info("remove_collection_deployments: " + str(resp))
        except ApiException as e:
            print("Exception when calling ExtensionsV1beta1Api->delete_collection_namespaced_deployment: " + str(e))
            status = False
            message = str(e)

        LOG.info("Deleting services")
        # Delete services
        k8s_beta = client.CoreV1Api()
        try: 
            resp = k8s_beta.list_namespaced_services(namespace, label_selector="service_uuid=" + service_uuid)
        except ApiException as e:
            LOG.info("Exception when calling CoreV1Api->list_namespaced_services: " + str(e))
            status = False
            message = str(e)

        k8s_services = []
        for k8s_service_list in resp.items:
            k8s_services.append(k8s_service_list.metadata.name)

        LOG.info("k8s service list" + str(k8s_services))
        for k8s_service in k8s_services:
            try: 
                resp = k8s_beta.delete_namespaced_service(namespace, name=k8s_service)
            except ApiException as e:
                LOG.info("Exception when calling CoreV1Api->delete_namespaced_service: " + str(e))            
                status = False
                message = str(e)                

        LOG.info("Deleting configmaps")
        # Delete configmaps
        k8s_beta = client.CoreV1Api()
        try: 
            resp = k8s_beta.delete_collection_namespaced_config_map(namespace, label_selector="service_uuid=" + service_uuid)
        except ApiException as e:
            LOG.info("Exception when calling CoreV1Api->delete_collection_namespaced_config_map: " + str(e))
            status = False
            message = str(e)

        return status, message

    def check_pod_names(self, deployment_selector, namespace, watch=False, include_uninitialized=True, pretty='True'):
        k8s_beta = client.CoreV1Api()
        resp = k8s_beta.list_namespaced_pod(label_selector="deployment=" + deployment_selector,
                        namespace=namespace , async_req=False)
        
        # TODO: for now just 1 replica. In future we will need the all PODs names
        cdu_reference_list = None
        for item in resp.items:
            cdu_reference = item.metadata.name
        #    cdu_reference_list.append(cdu_reference)
            
        #LOG.info("reply: " + str(cdu_reference_list))
        
        return cdu_reference

    def deployment_object(self, instance_uuid, cnf_yaml, service_uuid):
        """
        CNF modeling method. This build a deployment object in kubernetes
        instance_uuid: k8s deployment name
        cnf_yaml: CNF Descriptor in yaml format
        """
        LOG.info("CNFD: " + str(cnf_yaml))
        container_list = []
        deployment_k8s = None
        env_vars = None
        env_from = None
        if "cloudnative_deployment_units" in cnf_yaml:
            cdu = cnf_yaml.get('cloudnative_deployment_units')
            for cdu_obj in cdu:
                port_list = []
                environment = []
                cdu_id = cdu_obj.get('id')
                image = cdu_obj.get('image')
                cdu_conex = cdu_obj.get('connection_points')
                container_name = cdu_id
                config_map_id = cdu_id
                if cdu_obj.get('parameters'):
                    env_vars = cdu_obj['parameters'].get('env')
                if cdu_conex:
                    for po in cdu_conex:
                        port = po.get('port')
                        port_name = po.get('id')
                        port_list.append(client.V1ContainerPort(container_port=port, name=port_name))

                # Environment variables from descriptor
                if env_vars:
                    LOG.info("configmap: " + str(config_map_id))
                    KubernetesWrapperEngine.create_configmap(self, config_map_id, instance_uuid, env_vars, service_uuid, namespace = "default")
                else:
                    env_vars = {"sonata": "rules"}
                    LOG.info("configmap else: " + str(config_map_id))
                    KubernetesWrapperEngine.create_configmap(self, config_map_id, instance_uuid, env_vars, service_uuid, namespace = "default")
                env_from = client.V1EnvFromSource(config_map_ref = client.V1ConfigMapEnvSource(name = config_map_id, optional = False))

                # Default static environment variables
                environment.append(client.V1EnvVar(name="instance_uuid", value=instance_uuid))
                environment.append(client.V1EnvVar(name="service_uuid", value=service_uuid))

                # Configureate Pod template container
                container = client.V1Container(
                    env = environment,
                    name = container_name,
                    image = image,
                    ports = port_list,
                    env_from = [env_from])
                container_list.append(container)
        else:
            return deployment_k8s

        # LOG.info("Result: " + str(container_list))
        
        # Create and configurate a spec section
        deployment_label =  (str(cnf_yaml.get("vendor")) + "-" +
                             str(cnf_yaml.get("name")) + "-" +
                             str(cnf_yaml.get("version")) + "-" +
                             instance_uuid.split("-")[0]).replace(".", "-")
        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={'deployment': deployment_label,
                                                 'instance_uuid': cnf_yaml['instance_uuid'],
                                                 'service_uuid': service_uuid}
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
        return deployment_k8s

    def service_object(self, instance_uuid, cnf_yaml, deployment_selector, service_uuid):
        """
        CNF modeling method. This build a service object in kubernetes
        instance_uuid: function uuid
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
            metadata=client.V1ObjectMeta(name=deployment_selector, namespace="default", labels = {"service_uuid": service_uuid}),
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
        cpu_used = 0
        memory_used = 0 
        KubernetesWrapperEngine.get_vim_config(self, vim_uuid)
        LOG.info("vim_uuid: " + str(vim_uuid))
        api = client.ApiClient()
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
            LOG.info("CPU Used: " + str(cpu_used) + "Memory Used:" + str(memory_used))
        except ApiException as e:
            LOG.info("Exception when calling /apis/metrics.k8s.io/v1beta1/nodes: GET" + str(e))

        return (cpu_used, memory_used)

test = KubernetesWrapperEngine()