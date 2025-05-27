# MQTT-Streamlit-Client



## Features

* **MQTT Connection Management:**
    * Connect to any MQTT broker (defaulting to `localhost:1883`).
    * Configure broker address, port, custom client ID, username, and password.
    * Connect and Disconnect buttons with visual status indicators.
* **Message Publishing:**
    * **Manual Publish:** Send individual messages to a specified topic with configurable QoS and Retain flags.
    * **Periodic Publisher:**
        * Define multiple messages (Topic, Payload, QoS, Retain) in an interactive table.
        * **Import from CSV:** Upload a CSV file to populate the periodic messages list. *Sample file added for reference : Messages1.csv,Messages2.csv
        * Configure the publishing interval in seconds.
        * Start and Stop the periodic publishing thread.
* **Message Subscription & Monitoring:**
    * Subscribe to multiple MQTT topics with a specified QoS.
    * View a list of active subscriptions with an option to unsubscribe.
    * Display received messages in a real-time updating table, showing Serial No., Timestamp, Topic, and Payload.
    * **Export to CSV:** Download all received messages as a CSV file.
* **Limitation:**
    * **Recieved Message & Export:** Supports fixed format.Post processing of i.e. JSON/XML of payload not available. 
    * **Refresh on recieved message:** Page need to be refresh by pressing button for refresh to see, to see updated new message.
    * **Error Message:** Error message of Rerun pops-up after connect but it does not have any operational issue. Just ignore it. 
     

## Getting Started:
Download code from github https://github.com/harshaldosh/MQTT-Streamlit-Client.git

Extract and go to the folder

```shell
pip install -r requirements.txt
```

**Running the code**
if you want to see the basics and wanted to understand how the call back functions is being called use `MQTTClient.py`. It has Logs for new begineer to understand the flow. 
```shell
streamlit run MQTTClient.py
```

Once you know the basics, run full fledge thread based working client with option to send periodic data. `MQTTClient_th.py`. It has Logs for new begineer to understand the flow. 
```shell
streamlit run MQTTClient_th.py
```

### Prerequisites
Python
PIP