import streamlit as st
import pandas as pd
import paho.mqtt.client as paho
import time
import threading
import uuid
from datetime import datetime
import io # For CSV export/import operations
import json # For JSON parsing

# --- MQTT Client Class (Encapsulated) ---
class MqttClient:
    """
    A simple MQTT client class to handle connections, subscriptions, and publishing.
    """
    def __init__(self, broker, port, client_id="", username=None, password=None):
        self.broker = broker
        self.port = port
        # Generate a unique client ID if not provided
        self.client_id = client_id if client_id else f"streamlit_mqtt_{uuid.uuid4()}"
        self.username = username
        self.password = password
        # Initialize paho.mqtt.client with MQTT v5
        self.client = paho.Client(paho.CallbackAPIVersion.VERSION2, client_id=self.client_id)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_publish = self._on_publish
        self.is_connected = False
        self.messages_received = [] # To store received messages
        self.json_messages = [] # To store parsed JSON messages

        if self.username and self.password:
            self.client.username_pw_set(self.username, self.password)

    def _on_connect(self, client, userdata, flags, rc, properties):
        """Callback for when the client connects to the broker."""
        if rc == 0:
            self.is_connected = True
            st.success(f"Connected to MQTT Broker: {self.broker}:{self.port}")
            print("Connected to MQTT Broker!") # For console debugging
        else:
            self.is_connected = False
            st.error(f"Failed to connect, return code {rc}")
            print(f"Failed to connect, return code {rc}\n") # For console debugging

    def _on_message(self, client, userdata, msg):
        """Callback for when a message is received from the broker."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            payload = msg.payload.decode('utf-8')
        except UnicodeDecodeError:
            payload = f"Non-UTF-8 payload (raw: {msg.payload})"

        self.messages_received.append({
            "Serial No.": len(self.messages_received) + 1,
            "Timestamp": timestamp,
            "Topic": msg.topic,
            "Payload": payload
        })
        
        # Try to parse JSON payload
        try:
            json_data = json.loads(payload)
            # Flatten JSON for table display
            flattened_json = self._flatten_json(json_data)
            self.json_messages.append({
                "Serial No.": len(self.json_messages) + 1,
                "Timestamp": timestamp,
                "Topic": msg.topic,
                "JSON Data": json_data,
                **flattened_json  # Add flattened key-value pairs
            })
        except (json.JSONDecodeError, TypeError):
            # Not a valid JSON, skip JSON parsing
            pass
            
        # Note: Avoid printing to console here in Streamlit as it can be overwhelming
        # st.rerun() # DO NOT call rerun directly from callback. It causes issues.
                     # Updates are handled by Streamlit's natural reruns or a loop.

    def _on_publish(self, client, userdata, mid, reason_code, properties):
        """Callback for when a message is published."""
        # This callback is optional; can be used for debugging published messages
        pass

    def _flatten_json(self, json_obj, parent_key='', sep='_'):
        """
        Flatten a nested JSON object for table display.
        """
        items = []
        if isinstance(json_obj, dict):
            for k, v in json_obj.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                if isinstance(v, dict):
                    items.extend(self._flatten_json(v, new_key, sep=sep).items())
                elif isinstance(v, list):
                    for i, item in enumerate(v):
                        if isinstance(item, dict):
                            items.extend(self._flatten_json(item, f"{new_key}{sep}{i}", sep=sep).items())
                        else:
                            items.append((f"{new_key}{sep}{i}", item))
                else:
                    items.append((new_key, v))
        return dict(items)

    def connect(self):
        """Connects the MQTT client to the broker."""
        try:
            self.client.connect(self.broker, self.port, 60)
            self.client.loop_start() # Start a new thread that calls loop() for you
            time.sleep(1) # Give some time for connection to establish
        except Exception as e:
            st.error(f"Error connecting to MQTT broker: {e}")
            print(f"Error connecting to MQTT broker: {e}") # For console debugging
            self.is_connected = False

    def disconnect(self):
        """Disconnects the MQTT client from the broker."""
        if self.is_connected:
            self.client.loop_stop()
            self.client.disconnect()
            self.is_connected = False
            print("Disconnected from MQTT Broker.") # For console debugging

    def publish(self, topic, payload, qos=0, retain=False):
        """Publishs a message to a given topic."""
        if self.is_connected:
            self.client.publish(topic, payload, qos, retain)
            # st.success(f"Published '{payload}' to '{topic}'") # Can be noisy
        else:
            st.warning("Not connected to MQTT broker. Cannot publish.")

    def subscribe(self, topic, qos=0):
        """Subscribes to a given topic."""
        if self.is_connected:
            result, mid = self.client.subscribe(topic, qos)
            if result == paho.MQTT_ERR_SUCCESS:
                st.info(f"Subscription request sent for topic: '{topic}'")
            else:
                st.error(f"Failed to send subscription request for '{topic}': {result}")
        else:
            st.warning("Not connected to MQTT broker. Cannot subscribe.")

    def unsubscribe(self, topic):
        """Unsubscribes from a given topic."""
        if self.is_connected:
            self.client.unsubscribe(topic)
            st.info(f"Unsubscribed from topic: '{topic}'")
        else:
            st.warning("Not connected to MQTT broker. Cannot unsubscribe.")

    def get_received_messages(self):
        """Returns the list of messages received so far."""
        return self.messages_received

    def get_json_messages(self):
        """Returns the list of parsed JSON messages received so far."""
        return self.json_messages

# --- Streamlit Page Configuration ---
st.set_page_config(layout="wide", page_title="MQTT Client")

# --- Session State Initialization ---
# This ensures that variables persist across reruns of the Streamlit app
if 'mqtt_client' not in st.session_state:
    st.session_state.mqtt_client = None
if 'is_mqtt_connected' not in st.session_state:
    st.session_state.is_mqtt_connected = False
if 'subscribed_topics' not in st.session_state:
    st.session_state.subscribed_topics = set()
if 'publish_thread_running' not in st.session_state:
    st.session_state.publish_thread_running = False
if 'stop_publish_event' not in st.session_state:
    st.session_state.stop_publish_event = threading.Event()
if 'messages_df' not in st.session_state:
    st.session_state.messages_df = pd.DataFrame(columns=["Serial No.", "Timestamp", "Topic", "Payload"])
if 'json_messages_df' not in st.session_state:
    st.session_state.json_messages_df = pd.DataFrame()
if 'auto_publish_messages' not in st.session_state:
    st.session_state.auto_publish_messages = [
        {"Topic": "test/message", "Payload": "Message 1", "QoS": 0, "Retain": False},
        {"Topic": "test/message", "Payload": "Message 2", "QoS": 1, "Retain": True}
    ]
if 'broker_address' not in st.session_state:
    st.session_state.broker_address = "localhost" # Default public broker (changed from localhost for easier testing)
if 'broker_port' not in st.session_state:
    st.session_state.broker_port = 1883
if 'client_id' not in st.session_state:
    st.session_state.client_id = "" # Will generate if empty
if 'username' not in st.session_state:
    st.session_state.username = ""
if 'password' not in st.session_state:
    st.session_state.password = ""
if 'show_password' not in st.session_state:
    st.session_state.show_password = False
if 'last_message_count' not in st.session_state: # Added for more robust message display update
    st.session_state.last_message_count = 0
if 'last_json_message_count' not in st.session_state:
    st.session_state.last_json_message_count = 0
if 'auto_refresh_messages' not in st.session_state:
    st.session_state.auto_refresh_messages = False


# --- Helper Functions for UI Interactions ---

def connect_mqtt_ui():
    """Handles the MQTT connection logic based on UI inputs."""
    broker = st.session_state.broker_address
    port = st.session_state.broker_port
    client_id = st.session_state.client_id
    username = st.session_state.username if st.session_state.username else None
    password = st.session_state.password if st.session_state.password else None

    if st.session_state.mqtt_client and st.session_state.mqtt_client.is_connected:
        st.session_state.mqtt_client.disconnect() # Disconnect existing if any

    st.session_state.mqtt_client = MqttClient(broker, port, client_id, username, password)
    st.session_state.mqtt_client.connect()
    st.session_state.is_mqtt_connected = st.session_state.mqtt_client.is_connected
    # A Streamlit rerun will update the UI components based on this state change.
    st.rerun()

def disconnect_mqtt_ui():
    """Handles the MQTT disconnection logic."""
    if st.session_state.mqtt_client:
        st.session_state.mqtt_client.disconnect()
        st.session_state.is_mqtt_connected = False
        st.session_state.subscribed_topics.clear()
        if st.session_state.publish_thread_running:
            st.session_state.stop_publish_event.set() # Signal publisher thread to stop
            time.sleep(0.1) # Give a small moment for the thread to register the signal
        st.session_state.publish_thread_running = False
        st.session_state.stop_publish_event.clear() # Reset for next run
        st.session_state.messages_df = pd.DataFrame(columns=["Serial No.", "Timestamp", "Topic", "Payload"]) # Clear messages
        st.session_state.last_message_count = 0 # Reset message count
        st.session_state.json_messages_df = pd.DataFrame() # Clear JSON messages
        st.session_state.last_json_message_count = 0 # Reset JSON message count
        st.success("Disconnected from MQTT Broker.")
    st.rerun() # Rerun to update UI state

def subscribe_topic_ui():
    """Handles subscribing to a topic from UI."""
    # Get current values from session state instead of using parameters
    topic = st.session_state.get('subscribe_topic_input', '')
    qos = st.session_state.get('subscribe_qos_input', 0)
    
    if st.session_state.mqtt_client and st.session_state.is_mqtt_connected:
        st.session_state.mqtt_client.subscribe(topic, qos)
        st.session_state.subscribed_topics.add(topic)
        st.rerun()
    else:
        st.warning("Not connected to MQTT broker. Connect first to subscribe.")

def unsubscribe_topic_ui(topic):
    """Handles unsubscribing from a topic from UI."""
    if st.session_state.mqtt_client and st.session_state.is_mqtt_connected:
        st.session_state.mqtt_client.unsubscribe(topic)
        st.session_state.subscribed_topics.discard(topic)
        st.rerun()
    else:
        st.warning("Not connected to MQTT broker.")

def publish_message_ui(topic, payload, qos, retain):
    """Handles publishing a single message from UI."""
def publish_message_ui():
    """Handles publishing a single message from UI."""
    # Get current values from session state
    topic = st.session_state.get('publish_topic_input', '')
    payload = st.session_state.get('publish_payload_input', '')
    qos = st.session_state.get('publish_qos_input', 0)
    retain = st.session_state.get('publish_retain_input', False)
    
    if st.session_state.mqtt_client and st.session_state.is_mqtt_connected:
        st.session_state.mqtt_client.publish(topic, payload, qos, retain)
        st.toast(f"Published to '{topic}'") # Use toast for less intrusive feedback
    else:
        st.warning("Not connected to MQTT broker. Connect first to publish.")

def start_periodic_publisher():
    """Starts a background thread for periodic publishing."""
    # Get current values from session state
    messages_to_publish = st.session_state.auto_publish_messages
    interval = st.session_state.get('publish_interval_input', 1.0)
    stop_event = st.session_state.stop_publish_event
    
    if st.session_state.mqtt_client and st.session_state.is_mqtt_connected:
        if not st.session_state.publish_thread_running:
            stop_event.clear() # Clear any previous stop signal
            publisher_thread = threading.Thread(
                target=periodic_publisher_task,
                args=(st.session_state.mqtt_client, messages_to_publish, interval, stop_event)
            )
            publisher_thread.daemon = True # Allow the thread to exit with the main program
            publisher_thread.start()
            st.session_state.publish_thread_running = True
            st.success("Started periodic publisher.")
            st.rerun() # Rerun to update button state
        else:
            st.warning("Periodic publisher is already running.")
    else:
        st.warning("Not connected to MQTT broker. Cannot start publisher.")

def stop_periodic_publisher():
    """Stops the background thread for periodic publishing."""
    if st.session_state.publish_thread_running:
        st.session_state.stop_publish_event.set() # Signal the thread to stop
        st.session_state.publish_thread_running = False
        st.info("Stopping periodic publisher...")
        st.rerun() # Rerun to update button state
    else:
        st.info("Periodic publisher is not running.")

def periodic_publisher_task(mqtt_client_instance, messages, interval, stop_event):
    """The target function for the periodic publishing thread."""
    while not stop_event.is_set():
        for msg in messages:
            if stop_event.is_set():
                break # Exit if stop signal received during iteration
            mqtt_client_instance.publish(msg['Topic'], msg['Payload'], qos=msg.get('QoS', 0), retain=msg.get('Retain', False))
            time.sleep(interval)
    print("Periodic publisher thread gracefully terminated.") # For console debugging

def load_periodic_messages_from_csv(uploaded_file):
    """Loads periodic messages from an uploaded CSV file."""
    try:
        df = pd.read_csv(uploaded_file)
        expected_cols = {"Topic", "Payload", "QoS", "Retain"}
        
        # Check for missing required columns
        missing_cols = list(expected_cols - set(df.columns))
        if missing_cols:
            st.error(f"Error: CSV is missing required columns: {', '.join(missing_cols)}. Please ensure your CSV has 'Topic', 'Payload', 'QoS', 'Retain'.")
            return
        
        # Select only the expected columns and make a copy to avoid SettingWithCopyWarning
        df_processed = df[list(expected_cols)].copy()

        # Robust conversion for QoS
        df_processed["QoS"] = pd.to_numeric(df_processed["QoS"], errors='coerce')
        if df_processed["QoS"].isnull().any():
            st.warning("Warning: Some 'QoS' values in CSV could not be converted to numbers. Defaulting to 0 for those rows.")
        df_processed["QoS"] = df_processed["QoS"].fillna(0).astype(int)

        # Robust conversion for Retain
        # Convert to string, then lower, then map explicit 'true'/'1' to True, others to False
        df_processed["Retain"] = df_processed["Retain"].astype(str).str.lower().isin(['true', '1'])

        messages_list = df_processed.to_dict(orient="records")
        
        if not messages_list:
            st.warning("The CSV file was processed, but no valid messages were found after parsing. Please check the CSV content and formatting.")
            st.session_state.auto_publish_messages = [] # Explicitly ensure it's empty
        else:
            st.session_state.auto_publish_messages = messages_list
            st.success(f"Successfully loaded {len(messages_list)} messages from CSV.")
        
        
        
        #st.rerun() # Trigger a rerun to update the UI based on the new session state
    except pd.errors.EmptyDataError:
        st.error("Error: The uploaded CSV file is empty.")
    except pd.errors.ParserError:
        st.error("Error: Could not parse the CSV file. Please ensure it is a valid CSV format.")
    except Exception as e:
        st.error(f"An unexpected error occurred while processing the CSV: {e}")


# --- UI Layout ---

st.title("MQTT Client")
st.sidebar.header("MQTT Connection")
with st.sidebar.form("mqtt_config_form"):
    st.session_state.broker_address = st.text_input("Broker Address", value=st.session_state.broker_address, key="broker_input")
    st.session_state.broker_port = st.number_input("Port", value=st.session_state.broker_port, min_value=1, max_value=65535, key="port_input")
    st.session_state.client_id = st.text_input("Client ID (leave blank for auto-generate)", value=st.session_state.client_id, key="client_id_input")
    st.session_state.username = st.text_input("Username (optional)", value=st.session_state.username, key="username_input")

    password_type = "text" if st.session_state.show_password else "password"
    st.session_state.password = st.text_input("Password (optional)", type=password_type, value=st.session_state.password, key="password_input")
    st.session_state.show_password = st.checkbox("Show Password", value=st.session_state.show_password, key="show_password_checkbox")

    col_connect, col_disconnect = st.columns(2)
    with col_connect:
        connect_button = st.form_submit_button(
            "Connect",
            on_click=connect_mqtt_ui,
            disabled=st.session_state.is_mqtt_connected
        )
    with col_disconnect:
        disconnect_button = st.form_submit_button(
            "Disconnect",
            on_click=disconnect_mqtt_ui,
            disabled=not st.session_state.is_mqtt_connected
        )


# Main content area
col1, col2 = st.columns([1, 1.5]) # Adjusted column widths for better balance

with col1:
    st.header("Publish Messages")
    st.write("---") # Separator

    st.subheader("Manual Publish")
    MQTT_JSON = { "temperature": 25.5, "humidity": 60, "sensor": {
        "id": "temp01",
        "location": "room1"
    }}
    MQTT_JSON_Message = json.dumps(MQTT_JSON, indent=1)
    with st.form("publish_form"):
        publish_topic = st.text_input("Topic", value="test/message", key="publish_topic_input")
        publish_payload = st.text_area("Payload", value=MQTT_JSON_Message, key="publish_payload_input")
        col_qos_retain, col_manual_publish_btn = st.columns([1, 1])
        with col_qos_retain:
            publish_qos = st.selectbox("QoS", options=[0, 1, 2], index=0, key="publish_qos_input")
            publish_retain = st.checkbox("Retain", value=False, key="publish_retain_input")
        with col_manual_publish_btn:
            st.markdown("<br>", unsafe_allow_html=True) # Spacer
            st.form_submit_button(
                "Publish",
                on_click=publish_message_ui,
                disabled=not st.session_state.is_mqtt_connected
            )

    st.subheader("Periodic Publisher")
    with st.expander("Configure Periodic Publishing"):
        st.info("Manually add messages or upload a CSV file.")

        # --- CSV Upload for Periodic Messages ---
        uploaded_file = st.file_uploader(
            "Upload CSV for Periodic Messages (Cols: Topic, Payload, QoS, Retain)",
            type=["csv"],
            key="periodic_csv_uploader"
        )
        if uploaded_file is not None:
            load_periodic_messages_from_csv(uploaded_file)
            # After loading, the file_uploader automatically resets on rerun
            # so there's no need to explicitly set uploaded_file to None.

        st.markdown("**Current Periodic Messages:**")
        # Display and manage auto_publish_messages using Streamlit's data editor
        st.session_state.auto_publish_messages = st.data_editor(
            st.session_state.auto_publish_messages,
            num_rows="dynamic", # Allows adding/deleting rows directly in UI
            column_config={
                "Topic": st.column_config.TextColumn("Topic", required=True),
                "Payload": st.column_config.TextColumn("Payload", required=True),
                "QoS": st.column_config.SelectboxColumn("QoS", options=[0, 1, 2], default=0),
                "Retain": st.column_config.CheckboxColumn("Retain", default=False)
            }
            #key="auto_publish_data_editor"
        )

        publish_interval = st.number_input(
            "Publish Interval (seconds)",
            min_value=0.1,
            value=1.0,
            step=0.1,
            format="%.1f",
            key="publish_interval_input"
        )

        col_start_stop, col_status = st.columns([1, 1])
        with col_start_stop:
            start_periodic_button = st.button(
                "Start Periodic Publish",
                on_click=start_periodic_publisher,
                # Check for empty messages explicitly
                disabled=not st.session_state.is_mqtt_connected or st.session_state.publish_thread_running or len(st.session_state.auto_publish_messages) == 0,
                key="start_periodic_button"
            )
            stop_periodic_button = st.button(
                "Stop Periodic Publish",
                on_click=stop_periodic_publisher,
                disabled=not st.session_state.publish_thread_running,
                key="stop_periodic_button"
            )
        with col_status:
            st.markdown("<br>", unsafe_allow_html=True) # Spacer
            if st.session_state.publish_thread_running:
                st.write("Status: :green[**Running**]")
            else:
                st.write("Status: :red[**Stopped**]")
            if len(st.session_state.auto_publish_messages) == 0:
                st.warning("Add messages (or upload CSV) to enable periodic publishing.")


with col2:
    st.header("Subscribe & Monitor Messages")
    st.write("---") # Separator

    st.subheader("Manage Subscriptions")
    with st.form("subscribe_form"):
        new_topic_to_subscribe = st.text_input("New Topic to Subscribe", value="test/message", key="subscribe_topic_input")
        subscribe_qos = st.selectbox("QoS for Subscription", options=[0, 1, 2], index=0, key="subscribe_qos_input")
        st.form_submit_button(
            "Subscribe",
            on_click=subscribe_topic_ui,
            disabled=not st.session_state.is_mqtt_connected or new_topic_to_subscribe in st.session_state.subscribed_topics
        )

    st.subheader("Active Subscriptions")
    if st.session_state.subscribed_topics:
        for topic in list(st.session_state.subscribed_topics): # Convert to list to allow modification during iteration
            col_topic, col_unsubscribe_btn = st.columns([3, 1])
            with col_topic:
                st.text_input(f"Subscribed Topic {topic}", value=topic, disabled=True, label_visibility="collapsed")
            with col_unsubscribe_btn:
                st.button(
                    "Unsubscribe",
                    key=f"unsubscribe_{topic}",
                    on_click=unsubscribe_topic_ui,
                    args=(topic,),
                    disabled=not st.session_state.is_mqtt_connected
                )
    else:
        st.info("No topics subscribed yet.")

    st.subheader("Received Messages")
    
    # Auto-refresh checkbox
    col_auto_refresh, col_manual_refresh = st.columns([1, 1])
    with col_auto_refresh:
        auto_refresh_enabled = st.checkbox(
            "Auto-refresh messages",
            value=st.session_state.auto_refresh_messages,
            help="Automatically refresh the message table every 2 seconds",
            key="auto_refresh_messages_checkbox"
        )
        st.session_state.auto_refresh_messages = auto_refresh_enabled
    
    with col_manual_refresh:
        if st.button("Manual Refresh", help="Manually refresh to get the latest messages"):
            st.rerun()
    
    # Placeholder for messages, will be updated periodically
    message_placeholder = st.empty()

    # --- Message Update Logic (Rely on Streamlit's natural reruns) ---
    # This section gets executed on every Streamlit rerun (e.g., user interaction, button click).
    # New messages received by the MQTT client's background thread will be collected here.
    if st.session_state.mqtt_client and st.session_state.is_mqtt_connected:
        new_messages = st.session_state.mqtt_client.get_received_messages()
        new_json_messages = st.session_state.mqtt_client.get_json_messages()
        
        # Only update DataFrame and re-render if there are new messages, or if it was empty
        # and now has messages. This helps prevent unnecessary dataframe re-creations.
        if new_messages and (len(new_messages) != len(st.session_state.messages_df) or st.session_state.messages_df.empty):
            st.session_state.messages_df = pd.DataFrame(new_messages)
            st.session_state.messages_df["Serial No."] = range(1, len(st.session_state.messages_df) + 1)
            st.session_state.messages_df = st.session_state.messages_df[["Serial No.", "Timestamp", "Topic", "Payload"]]
            
        # Update JSON messages DataFrame
        if new_json_messages and (len(new_json_messages) != len(st.session_state.json_messages_df) or st.session_state.json_messages_df.empty):
            st.session_state.json_messages_df = pd.DataFrame(new_json_messages)
            
        with message_placeholder.container():
            if not st.session_state.messages_df.empty:
                st.dataframe(st.session_state.messages_df, height=300, use_container_width=True)

                # --- Export Received Messages to CSV ---
                # Ensure data is encoded for download button
                csv_data = st.session_state.messages_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Export Received Messages to CSV",
                    data=csv_data,
                    file_name=f"mqtt_received_messages_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    disabled=st.session_state.messages_df.empty,
                    key="download_received_csv"
                )
            else:
                st.info("Waiting for messages...")
    else:
        with message_placeholder.container():
            st.info("Connect to the MQTT broker to start receiving messages.")

# Auto-refresh mechanism for received messages
if (st.session_state.auto_refresh_messages and 
    st.session_state.is_mqtt_connected):
    
    # Show auto-refresh status
    st.info("🔄 Auto-refresh enabled - Messages will update automatically every 2 seconds")
    
    # Import time for the delay
    import time
    
    # Wait 2 seconds before triggering refresh
    time.sleep(2)
    st.rerun()

elif (st.session_state.auto_refresh_messages and 
      not st.session_state.is_mqtt_connected):
    st.warning("⚠️ Auto-refresh is enabled but MQTT is not connected")

# Keep the original refresh button for manual MQTT event processing
if st.button("Refresh to process MQTT events"):
    st.rerun()