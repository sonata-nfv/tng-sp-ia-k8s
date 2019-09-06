[![Build Status](https://jenkins.sonata-nfv.eu/buildStatus/icon?job=tng-sp-ia-k8s/master)](https://jenkins.sonata-nfv.eu/job/tng-sp-ia-k8s)
[![Join the chat at https://gitter.im/sonata-nfv/Lobby](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/sonata-nfv/Lobby)

# 5GTANGO Kubernetes wrapper for NFV cloud-native deployments

Kubernetes Wrapper: Component in the SONATA framework that is responsible to manage the interface between MANO and deployed functions.

## Requires

* Docker

## Implementation

* implemented in Python 3.4
* dependecies: amqp-storm
* The main implementation can be found in: `tng-sp-ia-k8s/k8s.py`

## How to run it

* (follow the general README.md of this repository to setup and test your environment)
* To run the k8s wrapper locally, you need:
  * a running RabbitMQ broker (see general README.md of this repo for info on how to do this)
  * a running plugin manager connected to the broker (see general README.md of this repo for info on how to do this)
* Run the k8s wrapper (directly in your terminal not in a Docker container):
  * `python3.4 tng-sp-ia-k8s/k8s.py`
* Or: run the k8s wrapper (in a Docker container):
  * `docker build -t tng-sp-ia-k8s .`
  * `docker run -it --link broker:broker --name tng-sp-ia-k8s tng-sp-ia-k8s`

## Output

The output of the k8s wrapper should look like this:

```bash
INFO:k8s-wrapper:main:Starting IA Wrapper: 'k8s.KubernetesWrapper' ...
INFO:k8s-wrapper:main:Wrapper is connected to broker.
INFO:k8s-wrapper:main:Wrapper running...
```

It shows how the k8s wrapper connects to the broker, registers itself to the plugin manager and receives the instantiation event.

## Unit tests

* To run the unit tests of the k8s individually, run the following from the root of the repo:
  * `./test/test_tng-sp-ia-k8s.sh`

## Licensing

This 5GTANGO component is published under Apache 2.0 license. Please see the LICENSE file for more details.

#### Lead Developers

The following lead developers are responsible for this repository and have admin rights. They can, for example, merge pull requests.

- Felipe Vicens ([@felipevicens](https://github.com/felipevicens))

#### Feedback-Chanel
* You may use the mailing list [sonata-dev-list](mailto:sonata-dev@lists.atosresearch.eu)
* Please use the GitHub issues to report bugs.
