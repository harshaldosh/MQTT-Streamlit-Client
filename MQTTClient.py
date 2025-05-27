import streamlit as st
import paho.mqtt.client as mqtt
import pandas as pd
from datetime import datetime
import time

# Initialize session state variables
if 'mqtt_client' not in st.session_state:
    st.session_state['mqtt_client'] = None

if 'connected' not in st.session_state:
    st.session_state['connected'] = False

if 'logs' not in st.session_state:
    st.session_state['logs'] = []

if 'messages' not in st.session_state:
    st.session_state['messages'] = []

if 'subscribed_topics' not in st.session_state:
    st.session_state['subscribed_topics'] = []

# Append log helper
def append_log(msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    st.session_state['logs'].append(f"[{timestamp}] {msg}")

# MQTT Callbacks
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        st.session_state['connected'] = True
        append_log("Connected to MQTT Broker successfully.")
        # Subscribe to topics after connection
        for topic in st.session_state['subscribed_topics']:
            client.subscribe(topic)
            append_log(f"Subscribed to topic: {topic}")
    else:
        append_log(f"Failed to connect, return code {rc}")

def on_disconnect(client, userdata, rc):
    st.session_state['connected'] = False
    append_log("Disconnected from MQTT Broker.")

def on_message(client, userdata, msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    payload = msg.payload.decode()
    st.session_state['messages'].append({
        "Serial": len(st.session_state['messages']) + 1,
        "Timestamp": timestamp,
        "Topic": msg.topic,
        "Payload": payload
    })
    append_log(f"Message received on topic '{msg.topic}': {payload}")

# MQTT client setup function
def setup_mqtt_client(broker, port, username, password, topics):
    client = mqtt.Client()
    if username:
        client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.connect(broker, port, keepalive=60)
    st.session_state['subscribed_topics'] = topics
    return client

# Streamlit UI
st.title("Streamlit MQTT Client with Logs (No Threading)")

# MQTT Configuration Inputs
broker = st.text_input("Broker address", "localhost")
port = st.number_input("Port", value=1883)
username = st.text_input("Username (optional)")
password = st.text_input("Password (optional)", type="password")

# Subscribe topics input
topics_input = st.text_area("Subscribe to topics (comma separated)", "sensor")
topics = [t.strip() for t in topics_input.split(",") if t.strip()]

connect_button = st.button("Connect to MQTT Broker")

if connect_button:
    try:
        client = setup_mqtt_client(broker, port, username, password, topics)
        st.session_state['mqtt_client'] = client
        append_log("Attempting to connect to broker...")
        client.loop(timeout=1.0)  # Process network events once
        st.rerun()
    except Exception as e:
        append_log(f"Error connecting to broker: {e}")

# If connected, show message sending UI and periodically process MQTT events
if st.session_state['connected'] and st.session_state['mqtt_client']:
    client = st.session_state['mqtt_client']

    st.success(f"Connected to {broker}:{port}")

    # Message sending
    st.header("Publish MQTT Message")
    send_topic = st.text_input("Publish Topic", value=topics[0] if topics else "")
    send_message = st.text_area("Message to send")
    if st.button("Publish Message"):
        if send_topic and send_message:
            result = client.publish(send_topic, send_message)
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                append_log(f"Published message to topic '{send_topic}': {send_message}")
            else:
                append_log(f"Failed to publish message to topic '{send_topic}'")
        else:
            append_log("Publish topic or message is empty.")

    # Process MQTT network events to receive messages
    client.loop(timeout=1.0) 

    # Display received messages
    st.header("Received Messages")
    if st.session_state['messages']:
        df_msgs = pd.DataFrame(st.session_state['messages'])
        st.table(df_msgs)
    else:
        st.write("No messages received yet.")

else:
    st.info("Not connected to MQTT Broker.")

# Show logs window
st.header("MQTT Logs")
log_text = "\n".join(st.session_state['logs'])
st.text_area("Logs", value=log_text, height=200, max_chars=None, key="log_area")

# Auto-refresh button to keep MQTT loop running and update UI
if st.session_state['connected']:
    if st.button("Refresh to process MQTT events"):
        st.rerun()
