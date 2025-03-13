# Description:
The th2-script is a code, which contains a set of requests to the th2 components, executed in turns. The script can be written in any language that supports a common library. This script is written on python and contains three general types of actions.

* Requests to **the act** for sending messages that are executed synchronously. This means that we are sending a request and waiting for the result of its execution.

* Requests to **the check1** for message verification based on the expected results that are executed asynchronously. This means that we are not waiting for the check to be completed.

* Sending events to **the estore** queue to organize test results into a structure, or to supplement information in the report.

**Schema of a simplified example script:**

<img src="https://github.com/th2-net/th2-documentation/blob/master/images/demo-ver154-main/script-base-flow.png" data-canonical-src="https://github.com/th2-net/th2-documentation/blob/master/images/demo-ver154-main/script-base-flow.png"  />

## How to start:
**Schema(th2 environment) needed:** https://github.com/th2-net/th2-infra-schema-demo/tree/ver-1.6.1-main_scenario

Python 3.7-3.9 environment required. (Data services lib don't support 3.10 yet)
1. Change **configs** based on your **RabbitMQ** , **act** and **check1**
    1. Fill **grpc.json** in a folder **config** with the host, and the port of your **act** and **check1** pods. You can found it in Kubernetes Dashboard in Services tab or execute in kubectl - kubectl get services
    1. Fill **mq.json** in a folder **config** with **RabbitMQ exchange** and **routing key** from **script** to **estore**. You can find this queue in Kubernetes Dashboard in Config Maps tab - script-entry-point-app-config. 
    1. Fill **rabbitMQ.json** in a folder **config** with your **RabbitMQ credentials**. You can find these credentials in Kubernetes Dashboard in Config Maps tab - rabbit-mq-app-config.
2. Fill data services config (`configs/ds_config.yaml`) with data provider url
3. Install required packages described in **requirements.txt**
4. Start **run.py**
5. After run.py will be finished. Jupyter notebook will happen in your browser. Run this notebook to execute data services demo part.

## Test Scenario:

1. User1 submit passive buy order with Price=x and Size=30 - **Order1**
1. User1 receives an Execution Report with ExecType=0
1. User1 submit passive buy order with Price=x+1 and Size=10 - **Order2**
1. User1 receives an Execution Report with ExecType=0
1. User2 submit an aggressive sell IOC order with price=x-1 and Size=100 - **Order3**
    1. User1 receives an Execution Report with ExecType=F on trade between **Order2** and **Order3**
    1. User2 receives an Execution Report with ExecType=F on trade between **Order3** and **Order2**
    1. User1 receives an Execution Report with ExecType=F on trade between **Order1** and **Order3**
    1. User2 receives an Execution Report with ExecType=F on trade between **Order3** and **Order1**
    1. User2 receives an Execution Report with ExecType=C on expired **Order3**

## Examples:

This repository contains some independent examples in `examples` folder. 
Each example includes logic for solving specific issue and can be used as template for user scripts.

### Publish image as message

Images in `png` and `webp` formats can be published as messages into th2 storage for later viewing via `th2-rpt-viewer:5.2.9` (or higher version)

#### script location

[publish_image_as_message.py](examples/publish_image_as_message/publish_image_as_message.py)

#### requirements

The th2 components are required for uploading images  

<details>
<summary>script.yml</summary>

```yaml
apiVersion: th2.exactpro.com/v2
kind: Th2Box
metadata:
  name: script
spec:
  imageName: dev-script
  imageVersion: dev-script
  type: th2-script
  pins:
    mq:
      publishers:
      - name: to_mstore_proto
        attributes: [raw, publish]
  extendedSettings:
    k8sProbes: false
    externalBox:
      enabled: true
    hostNetwork: false
    service:
      enabled: false
  prometheus:
    enabled: false
```
</details>

<details>
<summary>mstore-tp.yml</summary>

```yaml
apiVersion: th2.exactpro.com/v2
kind: Th2CoreBox
metadata:
  name: mstore-tp
spec:
  disabled: false
  imageName: ghcr.io/th2-net/th2-mstore
  imageVersion: 5.9.0-dev
  type: th2-rpt-data-provider
  pins:
    mq:
      subscribers:
      - name: transport
        attributes:
        - transport-group
        - subscribe
      - name: proto
        attributes:
        - raw
        - subscribe
        linkTo:
          - box: script
            pin: to_mstore_proto
  extendedSettings:
    envVariables:
      JAVA_TOOL_OPTIONS: >
        -XX:+ExitOnOutOfMemoryError
        -XX:+UseContainerSupport
        -Ddatastax-java-driver.advanced.connection.init-query-timeout="5000 milliseconds"
        -Ddatastax-java-driver.basic.request.timeout="5 seconds"
    resources:
      limits:
        cpu: 1000m
        memory: 1000Mi
      requests:
        cpu: 200m
        memory: 200Mi
    service:
      enabled: false
```
</details>

th2 config files of the script component should be downloaded from the cluster config maps and put into `cfg` dir
* from `script-app-config` Config Map
  * box.json
  * mq.json
  * prometheus.json
* from `rabbit-mq-external-app-config` Config Map
  * rabbitMQ.json (from `rabbit-mq-external-app-config` Config Map)

#### run

```shell
export RABBITMQ_PASS=''
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
python examples/publish_image_as_message/publish_image_as_message.py --config 'cfg' --image-files 'images/image.png' 'images/image.webp' --session-alias 'image'
```