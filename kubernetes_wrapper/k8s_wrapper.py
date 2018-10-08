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
import logging
import threading
import concurrent.futures as pool
import uuid
import time
import os
from os import path
import yaml, json
import uuid
from kubernetes.client.rest import ApiException
from pprint import pprint

logging.basicConfig(level=logging.INFO)
logging.getLogger('pika').setLevel(logging.ERROR)
LOG = logging.getLogger("k8s-wrapper:k8s-wrapper")
LOG.setLevel(logging.DEBUG)
MAX_DEPLOYMENT_TIME = 5

class KubernetesWrapperEngine(object):

    def __init__(self, **kwargs):
        """
        Initialize cluster connection.
        :param app_id: string that identifies application

        """
        #self.cluster = cluster_id

        # trigger connection setup (without blocking)
        self.load_config()

        # Threading workers
        self.thrd_pool = pool.ThreadPoolExecutor(max_workers=100)
        # Track the workers
        self.tasks = []
        #self.get_pods()
        #self.get_nodes()
        #deployment = self.deployment_generator("name", "nginx", "80")
        #self.create_deployment("default", deployment)
        #self.get_deployment_status("default", "test")
        '''
        with open(path.join(path.dirname(__file__), "cnf.yaml")) as f:
            deployment = yaml.load(f)
            deployment_id = str(uuid.uuid4())
            deploy = self.deployment_object(deployment_id, deployment)
            pprint(deploy)
            deployment_selector = deploy.spec.template.metadata.labels.get("deployment")
            print (deployment_selector)
        with open(path.join(path.dirname(__file__), "cnf.yaml")) as f:       
            deployment = yaml.load(f)
            pprint(deployment)
            service = self.service_object(deployment_id, deployment, deployment_selector)
            print("Creating a Deployment")
            self.create_deployment(deploy, "default")
            print("Creating a Service")
            self.create_service(service, "default")
        '''

    def load_config(self):
        # TODO: Configure the config from database and store it in kubeconfig
        # https://github.com/kubernetes-client/python/blob/master/examples/multiple_clusters.py
        config.load_kube_config()
        
    def get_pods(self):
        v1 = client.CoreV1Api()
        LOG.info("Listing pods with their IPs:")
        ret = v1.read_node_status("k8s-worker-1", pretty="pretty")
        ret2 = v1.read_node_status("k8s-worker-1")
        pprint(ret)
        for i in ret2:
            print("%s\t%s\t%s" % (i.status.node_info, i.metadata.labels, i.metadata.name))

    def get_nodes(self):
        v1 = client.CoreV1Api()
        LOG.info("Listing pods with their IPs:")
        ret = v1.list_pod_for_all_namespaces(watch=False)
        for i in ret:
            print("%s\t%s\t%s" % (i.status.pod_ip, i.metadata.namespace, i.metadata.name))

    def create_deployment(self, deployment, namespace, watch=False, include_uninitialized=True, pretty='True' ):
        reply = {}
        status = None
        message = None
        k8s_beta = client.ExtensionsV1beta1Api()
        resp = k8s_beta.create_namespaced_deployment(
                        body=deployment, namespace=namespace , async_req=False)

        for iteration in range(MAX_DEPLOYMENT_TIME):
            LOG.info(iteration)
            if iteration == MAX_DEPLOYMENT_TIME - 1:
                status = "ERROR"
                message = "Deployment time exceeded"
                break
            try: 
                resp = k8s_beta.read_namespaced_deployment(resp.metadata.name, namespace=namespace, exact=False, export=False)
                pprint(resp)
                #print(len(resp.status.conditions))
                if resp.status.conditions:
                    if len(resp.status.conditions) >  1:
                        LOG.info("Deployment created. Status='%s' Message='%s' Reason='%s'" % 
                                ( str(resp.status.conditions[1].status), 
                                str(resp.status.conditions[1].message), 
                                str(resp.status.conditions[1].reason)))
                        if resp.status.conditions[1].status == "False":
                            status = "ERROR"
                            break
                        elif resp.status.conditions[1].status == "True":
                            if resp.status.conditions[1].reason == "ReplicaSetUpdated":
                                pass
                            elif resp.status.conditions[1].reason == "NewReplicaSetAvailable":
                                status = "COMPLETED"
                                message = None
                                break

            except ApiException as e:
                LOG.info("Exception when calling ExtensionsV1beta1Api->read_namespaced_deployment: %s\n" % e)
                reply['message'] = e
                reply['instanceName'] = str(resp.metadata.name)
                reply['instanceVimUuid'] = "unknown"
                reply['vimUuid'] = "unknown" 
                reply['request_status'] = "ERROR"
                reply['ip_mapping'] = None
                reply['vnfr'] = None
                LOG.info(str(reply))
                return reply
            time.sleep(2)
        
        reply['message'] = message
        reply['instanceName'] = str(resp.metadata.name)
        reply['instanceVimUuid'] = "unknown"
        reply['vimUuid'] = "unknown" 
        reply['request_status'] = status
        reply['ip_mapping'] = None
        reply['vnfr'] = None
        LOG.info(str(reply))
        return reply

    def create_service(self, service, namespace, watch=False, include_uninitialized=True, pretty='True' ):
        reply = {}
        status = None
        message = None
        k8s_beta = client.CoreV1Api()
        resp = k8s_beta.create_namespaced_service(
                        body=service, namespace=namespace , async_req=False)
        LOG.info(resp)
        message = "COMPLETED"
        '''
        for iteration in range(MAX_DEPLOYMENT_TIME):
            LOG.info(iteration)
            if iteration == MAX_DEPLOYMENT_TIME - 1:
                status = "ERROR"
                message = "Deployment time exceeded"
                break
            try: 
                resp = k8s_beta.read_namespaced_service(resp.metadata.name, namespace=namespace, exact=False, export=False)
                LOG.info(resp)

            except ApiException as e:
                LOG.info("Exception when calling ExtensionsV1beta1Api->read_namespaced_deployment: %s\n" % e)
                reply['message'] = e
                reply['instanceName'] = str(resp.metadata.name)
                reply['instanceVimUuid'] = "unknown"
                reply['vimUuid'] = "unknown" 
                reply['request_status'] = "ERROR"
                reply['ip_mapping'] = None
                reply['vnfr'] = resp
                LOG.info(str(reply))
                return reply
            time.sleep(2)
        '''
        reply['message'] = message
        reply['instanceName'] = str(resp.metadata.name)
        reply['instanceVimUuid'] = "unknown"
        reply['vimUuid'] = "unknown" 
        reply['request_status'] = status
        reply['ip_mapping'] = None
        reply['vnfr'] = None
        LOG.info(str(reply))
        return reply

    def get_deployment_status(self, namespace, deployment):
        k8s_beta = client.ExtensionsV1beta1Api()
        resp = k8s_beta.read_namespaced_deployment(
                        name=deployment, namespace=namespace)
        LOG.info("Deployment status='%s'" % str(resp.status))       
        pprint(resp)

    def deployment_generator(self, name, image, port):
        LOG.info("This deployment yaml will be generated by: %s\t%s\t%s" % (name, image, port))
        deployment = yaml.load("""
        apiVersion: extensions/v1beta1
        kind: Deployment
        metadata:
            name: test
        spec:
          template:
            metadata:
              labels:
                app: nginx
            spec:
              containers:
              - name: nginx
                image: nginx:1.7.9
                ports:
                - containerPort: 80
        """)
        LOG.info("This is the deployment file" + yaml.dump(deployment))
        return deployment
    
    def deployment_object(self, DEPLOYMENT_NAME, cnf_yaml):
        LOG.info("CNFD: " + str(cnf_yaml))
        if "cloudnative_deployment_units" in cnf_yaml:
            cdu = cnf_yaml.get('cloudnative_deployment_units')
            for cdu_obj in cdu:
                cdu_id = cdu_obj.get('id')
                cdu_image = cdu_obj.get('image')
                cdu_conex = cdu_obj.get('connection_points')
                container_name = cdu_id + "-" + DEPLOYMENT_NAME
                image = cdu_image
                port = cdu_conex[0].get('port')
        else:
            pass

        LOG.info("Result: " + container_name, image, port)
        
        # Configureate Pod template container
        container = client.V1Container(
            name=container_name,
            image=image,
            ports=[client.V1ContainerPort(container_port=80)])
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
        LOG.info("CNFD: " + str(cnf_yaml))
        ports=[]
        if cnf_yaml.get("connection_points"):
            for connection_points in cnf_yaml["connection_points"]:
                LOG.info("CONNECTION_POINTS:" + str(connection_points))
                for port_obj in connection_points.get("ports"):
                    LOG.info("PORT_OBJ" + str(port_obj))
                    for cdu_connection_points in cnf_yaml["cloudnative_deployment_units"]:
                        cdu_id,cdu_name = port_obj.get("target_port").split(":")
                        LOG.info(cdu_id, port_obj.get("port"), port_obj.get("id"))
                        LOG.info("CDU_PORT" + str(cdu_connection_points))
                        if cdu_connection_points.get("id") == cdu_id:
                            for cdu_ports in cdu_connection_points["connection_points"]:
                                if cdu_name == cdu_ports.get("id"):
                                    ports.append(client.V1ServicePort(
                                        name=cdu_name,
                                        port=port_obj.get("port"),
                                        target_port=cdu_ports.get("port")
                                        ))               
        LOG.info(ports)
        LOG.info("Deployment Selector:" + str(deployment_selector))
        
        # Create the specification of service
        spec = client.V1ServiceSpec(
            ports=[
                    {
                        "name": "http",
                        "port": 80,
                        "protocol": "TCP",
                        "targetPort": 80
                    }
                ],
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
        LOG.info(service)
        return service

test = KubernetesWrapperEngine()