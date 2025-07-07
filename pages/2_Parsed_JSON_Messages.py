import streamlit as st
import pandas as pd
import json
import time
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go

# --- Streamlit Page Configuration ---
st.set_page_config(layout="wide", page_title="Parsed JSON Messages")

# Initialize session state for graph settings
if 'auto_update_graph' not in st.session_state:
    st.session_state.auto_update_graph = False
if 'selected_x_axis' not in st.session_state:
    st.session_state.selected_x_axis = "Timestamp"
if 'selected_y_axes' not in st.session_state:
    st.session_state.selected_y_axes = []
if 'last_json_message_count' not in st.session_state:
    st.session_state.last_json_message_count = 0
if 'multi_graph_enabled' not in st.session_state:
    st.session_state.multi_graph_enabled = False

# --- Auto-refresh data from MQTT client ---
# Check if MQTT client exists and is connected, then fetch latest JSON messages
if ('mqtt_client' in st.session_state and 
    st.session_state.mqtt_client and 
    hasattr(st.session_state, 'is_mqtt_connected') and 
    st.session_state.is_mqtt_connected):
    
    # Get the latest JSON messages from the MQTT client
    latest_json_messages = st.session_state.mqtt_client.get_json_messages()
    
    # Update the JSON messages DataFrame if there are new messages
    if latest_json_messages:
        current_message_count = len(latest_json_messages)
        
        # Only update if we have new messages or if the DataFrame is empty
        if (current_message_count != st.session_state.last_json_message_count or 
            'json_messages_df' not in st.session_state or 
            st.session_state.json_messages_df.empty):
            
            st.session_state.json_messages_df = pd.DataFrame(latest_json_messages)
            st.session_state.last_json_message_count = current_message_count

st.title("Parsed JSON Messages")

# Check if we have JSON messages data
if 'json_messages_df' not in st.session_state or st.session_state.json_messages_df.empty:
    st.info("No JSON messages available. Please connect to the MQTT broker and receive some JSON messages first.")
    st.markdown("**Note:** JSON messages will appear here when valid JSON payloads are received on the MQTT Client page.")
    
    # Show connection status
    if ('mqtt_client' in st.session_state and 
        st.session_state.mqtt_client and 
        hasattr(st.session_state, 'is_mqtt_connected')):
        if st.session_state.is_mqtt_connected:
            st.success("MQTT Client is connected. Waiting for JSON messages...")
        else:
            st.warning("MQTT Client is not connected. Please connect on the MQTT Client page.")
    else:
        st.info("Please visit the MQTT Client page to establish a connection.")
        
else:
    # Display JSON messages
    st.subheader("JSON Messages Table")
    
    # Show message count and last update info
    col_info1, col_info2, col_info3 = st.columns(3)
    with col_info1:
        st.metric("Total JSON Messages", len(st.session_state.json_messages_df))
    with col_info2:
        if not st.session_state.json_messages_df.empty:
            last_message_time = st.session_state.json_messages_df.iloc[-1]['Timestamp']
            st.metric("Last Message", last_message_time)
    with col_info3:
        connection_status = "Connected" if (hasattr(st.session_state, 'is_mqtt_connected') and st.session_state.is_mqtt_connected) else "Disconnected"
        st.metric("MQTT Status", connection_status)
    
    # Create a display DataFrame without the raw JSON Data column for cleaner view
    display_df = st.session_state.json_messages_df.drop(columns=['JSON Data'], errors='ignore')
    st.dataframe(display_df, height=300, use_container_width=True)
    
    # Show expandable raw JSON data
    with st.expander("View Raw JSON Data"):
        for idx, row in st.session_state.json_messages_df.iterrows():
            st.write(f"**Message {row['Serial No.']} ({row['Timestamp']}) - Topic: {row['Topic']}**")
            st.json(row['JSON Data'])
            st.write("---")
    
    # --- Export JSON Messages to CSV ---
    json_csv_data = display_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Export JSON Messages to CSV",
        data=json_csv_data,
        file_name=f"mqtt_json_messages_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        disabled=st.session_state.json_messages_df.empty,
        key="download_json_csv"
    )
    
    # --- JSON Data Visualization ---
    st.subheader("JSON Data Visualization")
    
    # Identify data columns (excluding metadata columns)
    metadata_columns = {"Serial No.", "Timestamp", "Topic", "JSON Data"}
    data_columns = [col for col in display_df.columns if col not in metadata_columns]
    
    if not data_columns:
        st.info("No data columns found in JSON messages for visualization. JSON messages need to contain numeric data fields.")
    else:
        # Identify numeric columns for Y-axis
        numeric_columns = []
        for col in data_columns:
            try:
                # Try to convert to numeric, if successful, it's a numeric column
                pd.to_numeric(display_df[col], errors='coerce')
                numeric_columns.append(col)
            except:
                pass
        
        if not numeric_columns:
            st.info("No numeric columns found in JSON data for visualization.")
        else:
            col1, col2 = st.columns([1, 1])
            
            with col1:
                # X-Axis Selection
                x_axis_options = ["Timestamp"] + data_columns
                selected_x_axis = st.selectbox(
                    "Select X-Axis",
                    options=x_axis_options,
                    index=0 if "Timestamp" in x_axis_options else 0,
                    key="x_axis_select"
                )
                st.session_state.selected_x_axis = selected_x_axis
                
                # Y-Axis Selection (Multi-select)
                selected_y_axes = st.multiselect(
                    "Select Y-Axis (Multiple selection allowed)",
                    options=numeric_columns,
                    default=st.session_state.selected_y_axes if st.session_state.selected_y_axes else [],
                    key="y_axis_multiselect"
                )
                st.session_state.selected_y_axes = selected_y_axes
            
            with col2:
                # Auto Update Graph Checkbox
                auto_update = st.checkbox(
                    "Auto Update Graph",
                    value=st.session_state.auto_update_graph,
                    help="Automatically update the graph when new JSON messages are received",
                    key="auto_update_checkbox"
                )
                st.session_state.auto_update_graph = auto_update
                
                # Multi Graph Checkbox
                multi_graph = st.checkbox(
                    "Multi Graph Mode",
                    value=st.session_state.multi_graph_enabled,
                    help="Create separate graphs for each selected Y-axis variable",
                    key="multi_graph_checkbox"
                )
                st.session_state.multi_graph_enabled = multi_graph
                
                # Generate Graph Button
                generate_graph = st.button(
                    "Generate Graph",
                    type="primary",
                    key="generate_graph_button"
                )
            
            # Graph Generation Logic
            should_generate_graph = generate_graph or (auto_update and selected_y_axes)
            
            if should_generate_graph:
                if not selected_y_axes:
                    st.warning("Please select at least one Y-axis column to generate the graph.")
                else:
                    try:
                        # Prepare data for plotting
                        plot_df = display_df.copy()
                        
                        # Convert timestamp to datetime if X-axis is Timestamp
                        if selected_x_axis == "Timestamp":
                            try:
                                plot_df["Timestamp"] = pd.to_datetime(plot_df["Timestamp"])
                            except:
                                st.error("Could not convert Timestamp to datetime format for plotting.")
                                st.stop()
                        
                        # Convert Y-axis columns to numeric
                        for col in selected_y_axes:
                            plot_df[col] = pd.to_numeric(plot_df[col], errors='coerce')
                        
                        # Remove rows with NaN values in selected columns
                        plot_columns = [selected_x_axis] + selected_y_axes
                        plot_df = plot_df.dropna(subset=plot_columns)
                        
                        if plot_df.empty:
                            st.warning("No valid data points found for the selected columns after removing invalid values.")
                        else:
                            # Check if multi-graph mode is enabled
                            if st.session_state.multi_graph_enabled:
                                # Create separate graphs for each Y-axis
                                st.subheader("Multi-Graph Visualization")
                                
                                # Calculate number of columns for layout (max 2 columns)
                                num_graphs = len(selected_y_axes)
                                cols_per_row = min(2, num_graphs)
                                
                                # Create graphs in rows of up to 2 columns
                                for i in range(0, num_graphs, cols_per_row):
                                    # Create columns for this row
                                    if i + 1 < num_graphs and cols_per_row == 2:
                                        col_left, col_right = st.columns(2)
                                        columns = [col_left, col_right]
                                    else:
                                        columns = [st.columns(1)[0]]
                                    
                                    # Create graphs for this row
                                    for j, col in enumerate(columns):
                                        graph_index = i + j
                                        if graph_index < num_graphs:
                                            y_col = selected_y_axes[graph_index]
                                            
                                            with col:
                                                # Create individual graph
                                                fig = go.Figure()
                                                
                                                fig.add_trace(go.Scatter(
                                                    x=plot_df[selected_x_axis],
                                                    y=plot_df[y_col],
                                                    mode='lines+markers',
                                                    name=y_col,
                                                    line=dict(width=2),
                                                    marker=dict(size=6)
                                                ))
                                                
                                                # Update layout for individual graph
                                                fig.update_layout(
                                                    title=f"{y_col} vs {selected_x_axis}",
                                                    xaxis_title=selected_x_axis,
                                                    yaxis_title=y_col,
                                                    hovermode='x unified',
                                                    showlegend=False,  # Hide legend for individual graphs
                                                    height=400,
                                                    margin=dict(l=50, r=50, t=50, b=50)
                                                )
                                                
                                                # Display the individual plot
                                                st.plotly_chart(fig, use_container_width=True)
                            else:
                                # Create single combined plot (original behavior)
                                fig = go.Figure()
                                
                                # Add a line for each selected Y-axis
                                for y_col in selected_y_axes:
                                    fig.add_trace(go.Scatter(
                                        x=plot_df[selected_x_axis],
                                        y=plot_df[y_col],
                                        mode='lines+markers',
                                        name=y_col,
                                        line=dict(width=2),
                                        marker=dict(size=6)
                                    ))
                                
                                # Update layout
                                fig.update_layout(
                                    title=f"JSON Data Visualization: {', '.join(selected_y_axes)} vs {selected_x_axis}",
                                    xaxis_title=selected_x_axis,
                                    yaxis_title="Values",
                                    hovermode='x unified',
                                    showlegend=True,
                                    height=500
                                )
                                
                                # Display the plot
                                st.plotly_chart(fig, use_container_width=True)
                            
                            # Show data summary
                            st.subheader("Data Summary")
                            summary_df = plot_df[plot_columns].describe()
                            st.dataframe(summary_df, use_container_width=True)
                            
                    except Exception as e:
                        st.error(f"Error generating graph: {str(e)}")
            
            elif not selected_y_axes:
                st.info("Select Y-axis columns and click 'Generate Graph' or enable 'Auto Update Graph' to see the visualization.")

# Add refresh button for manual updates
col_refresh1, col_refresh2 = st.columns([1, 4])
with col_refresh1:
    if st.button("Refresh Data", help="Refresh to get the latest JSON messages"):
        st.rerun()

# --- Auto-refresh mechanism ---
# This section implements automatic page refresh when auto-update is enabled
if (hasattr(st.session_state, 'auto_update_graph') and 
    st.session_state.auto_update_graph and 
    hasattr(st.session_state, 'is_mqtt_connected') and 
    st.session_state.is_mqtt_connected):
    
    # Add a placeholder for auto-refresh status
    with col_refresh2:
        st.info("🔄 Auto-refresh enabled - Page will update automatically every 2 seconds")
    
    # Use st.empty() to create a placeholder for the auto-refresh timer
    auto_refresh_placeholder = st.empty()
    
    # Implement auto-refresh with a short delay
    time.sleep(2)  # Wait 2 seconds before triggering refresh
    st.rerun()  # Trigger page refresh to get latest data

elif (hasattr(st.session_state, 'auto_update_graph') and 
      st.session_state.auto_update_graph and 
      (not hasattr(st.session_state, 'is_mqtt_connected') or 
       not st.session_state.is_mqtt_connected)):
    
    with col_refresh2:
        st.warning("⚠️ Auto-refresh is enabled but MQTT is not connected")