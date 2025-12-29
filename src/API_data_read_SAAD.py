"""
Description:
    This script continuously fetches device data from a cloud API (Saicloud.in) using
    configured access tokens. It reads device configurations, including MQTT topics
    and parameter mappings, from a CSV file (config.csv). The fetched data is then
    formatted and published to an MQTT broker. The script automatically reloads
    configurations if the config.csv file is modified.

Author Information:
    Developer: SAAD IQBAL CHAUHAN
    Email: saadchavhan2@gmail.com
    Organization: PBR RESEARCH AMRAVATI
    Website: https://pbrresearch.com/
"""    
import requests # Used to make web requests to APIs
import json # Used to work with JSON data
import paho.mqtt.client as mqtt # Used to send messages to an MQTT broker
from time import sleep # Used to pause the script for a short time
import csv # Used to read data from CSV files
import os # Used to interact with the operating system, like checking file existence
import time # Used to get the last modification time of files

# The base URL for the API
host = "https://api.saicloud.in"
# The specific part of the URL to get device data
endpoint = "/device_api/device_data/5"
# The address of the MQTT message broker
mqtt_broker = "cloud.pbrresearch.com"
# The port number for the MQTT broker
mqtt_port = 1883
# Username for connecting to the MQTT broker
mqtt_username = "316A20004"
# Password for connecting to the MQTT broker
mqtt_password = "316A20004"
# -----------------------------------------------

# Function to send a message to the MQTT broker
def send_to_mqtt(broker, port, topic, parameter_value, username=None, password=None):
    try:
        # Create a new MQTT client instance
        client = mqtt.Client()

        # If a username and password are provided, set them for authentication
        if username and password:
            client.username_pw_set(username, password)

        # Connect to the MQTT broker
        client.connect(broker, port, 60)
        # Start the MQTT client's network loop in the background
        client.loop_start()

        # Convert the parameter value to a string for the MQTT payload
        payload = str(parameter_value)
        # Publish the message to the specified topic
        result = client.publish(topic, payload)

        # Check the status of the publish operation
        status = result[0]
        if status == 0:
            # Print success message if the message was sent
            print(f"✅ Sent `{payload}` to topic `{topic}`")
        else:
            # Print error message if sending failed
            print(f"❌ Failed to send message to topic {topic}")

        # Stop the MQTT client's network loop
        client.loop_stop()
        # Disconnect from the MQTT broker
        client.disconnect()

    except Exception as e:
        # Catch and print any errors during MQTT operations
        print(f"🔻 MQTT Error: {e}")

# Function to load device settings from a CSV file
# Modified to accept the full path to the config file
def load_device_configs_from_csv(filename_full_path):
    # List to store all device configurations
    device_configs = []
    # Check if the configuration file exists
    if not os.path.exists(filename_full_path):
        print(f"Error: Configuration file '{filename_full_path}' not found.")
        return None

    try:
        # Open the CSV file for reading
        with open(filename_full_path, mode='r', newline='') as file:
            # Create a CSV reader object
            reader = csv.reader(file)
            # The CSV file is expected to have no header row

            # Loop through each row in the CSV file
            for row in reader:
                # Check if the row has at least two columns (access_token and mqtt_topic)
                if len(row) >= 2:
                    # Get the access token from the first column
                    access_token = row[0].strip()
                    # Get the MQTT topic from the second column
                    mqtt_topic = row[1].strip()

                    # Dictionary to store how API parameters map to MQTT names
                    param_map = {}
                    # Loop through the rest of the columns for parameter mappings
                    for mapping_pair in row[2:]:
                        # Check if the mapping pair contains a colon (e.g., "api_key:mqtt_name")
                        if ':' in mapping_pair:
                            # Split the pair into API key and MQTT display name
                            api_key, mqtt_name = mapping_pair.split(':', 1)
                            # Store the mapping in the dictionary
                            param_map[api_key.strip()] = mqtt_name.strip()
                        else:
                            # Warn if a mapping pair is badly formatted
                            print(f"Warning: Malformed parameter mapping '{mapping_pair}' in row: {row}. Skipping.")

                    # If all necessary parts (token, topic, and mappings) are present
                    if access_token and mqtt_topic and param_map:
                        # Add the device configuration to the list
                        device_configs.append({
                            "access_token": access_token,
                            "mqtt_topic": mqtt_topic,
                            "parameter_map": param_map
                        })
                    else:
                        # Warn if a row is incomplete
                        print(f"Warning: Skipping incomplete row in CSV: {row}")
                else:
                    # Warn if a row has too few columns
                    print(f"Warning: Skipping malformed row in CSV (too few columns): {row}")
        # Return the list of loaded device configurations
        return device_configs
    except Exception as e:
        # Catch and print any errors during CSV loading
        print(f"Error loading device configurations from CSV: {e}")
        return None

# Function to fetch data for one device and publish its mapped parameters
def fetch_and_publish_device_data(device_config):
    # Get the access token from the device configuration
    token = device_config["access_token"]
    # Get the MQTT topic from the device configuration
    mqtt_topic = device_config["mqtt_topic"]
    # Get the parameter mapping from the device configuration
    parameter_map = device_config["parameter_map"]

    # Set the HTTP headers for the API request
    headers = {
        "Access-Token": token
    }
    try:
        # Construct the full API URL
        url = host + endpoint
        # Print a message indicating connection attempt
        print(f"\n🔗 Connecting to {url} for token: {token[:8]}...")
        # Make a GET request to the API
        response = requests.get(url, headers=headers, timeout=10)

        # Check if the HTTP status code is not 200 (OK)
        if response.status_code != 200:
            # Print an error if data fetching failed
            print(f"❌ Failed to fetch data for token {token[:8]}.... HTTP Status: {response.status_code}")
            # Print the response text for more details
            print(response.text)
            return

        # Parse the JSON response from the API
        data = response.json()
        # Print a success message
        print(f"✅ Successfully received data for token {token[:8]}....")

        # Dictionary to store parameters received from the API
        params_from_api = {}
        # Check if the 'data' key exists and is not empty in the API response
        if 'data' in data and len(data['data']) > 0:
            # Loop through each item in the 'data' array
            for item in data['data']:
                # Get the 'parameter_key' and 'parameter_value'
                key = item.get('parameter_key')
                value = item.get('parameter_value')
                # If both key and value are present, add them to the dictionary
                if key and value is not None:
                    params_from_api[key] = value
        else:
            # Print a message if no data was found in the API response
            print(f"No 'data' array or array is empty in API response for token {token[:8]}....")

        # List to store parts of the MQTT message
        msg_parts = []
        # Loop through the parameter mappings defined in the CSV
        for api_key_from_csv, mqtt_display_name in parameter_map.items():
            # Get the value from the API response, or 'N/A' if not found
            value_to_send = params_from_api.get(api_key_from_csv, 'N/A')
            # Add the formatted parameter to the message parts list
            msg_parts.append(f"{mqtt_display_name}:{value_to_send}")

        # Join all message parts with a comma to form the final MQTT message
        final_mqtt_message = ",".join(msg_parts)

        # Print the extracted parameters
        print(f"Extracted Parameters for topic '{mqtt_topic}':")
        print(final_mqtt_message)

        # Send the final message to the MQTT broker
        send_to_mqtt(
            broker=mqtt_broker,
            port=mqtt_port,
            topic=mqtt_topic,
            parameter_value = final_mqtt_message,
            username=mqtt_username,
            password=mqtt_password
        )

    except requests.exceptions.RequestException as e:
        # Catch and print errors related to API connection issues
        print(f"🔻 Error connecting to API for token {token[:8]}....: {e}")
    except Exception as e:
        # Catch and print any other unexpected errors
        print(f"🔻 An unexpected error occurred in fetch_and_publish_device_data for token {token[:8]}....: {e}")


# This block runs when the script is executed directly
if __name__ == "__main__":
    # --- START OF MODIFIED SECTION ---
    # Get the directory where the current script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Define the name of the configuration file
    CONFIG_FILENAME = "config.csv"
    # Construct the full path to the configuration file
    CONFIG_FILE_PATH = os.path.join(script_dir, CONFIG_FILENAME)
    # --- END OF MODIFIED SECTION ---

    # List to hold device configurations
    device_configs = []
    # Variable to store the last time the CSV file was modified
    last_modified_time = None

    # Initial attempt to load configurations from the CSV file
    # Use the full path here
    print(f"Attempting initial load of {CONFIG_FILENAME}...")
    temp_configs = load_device_configs_from_csv(CONFIG_FILE_PATH) # Pass full path
    if temp_configs:
        # If configs are loaded, store them and their modification time
        device_configs = temp_configs
        # Get modification time using the full path
        last_modified_time = os.path.getmtime(CONFIG_FILE_PATH)
        print(f"Successfully loaded {len(device_configs)} configurations.")
    else:
        # If no valid configs were loaded initially
        print("No valid configurations loaded during initial attempt. Please check config.csv.")
        # The loop below will keep trying to load if no configs are present

    # Main loop that runs indefinitely
    while 1:
        # Variable to store the current modification time of the CSV file
        current_modified_time = None
        # Check if the config file exists using the full path
        if os.path.exists(CONFIG_FILE_PATH):
            # Get the current modification time of the file using the full path
            current_modified_time = os.path.getmtime(CONFIG_FILE_PATH)

        # Reload configurations if the file doesn't exist anymore or if it has been modified
        if current_modified_time is None or current_modified_time != last_modified_time:
            print(f"\n💡 {CONFIG_FILENAME} detected change or re-appeared. Attempting to reload configurations...")
            # Try to reload the configurations using the full path
            reloaded_configs = load_device_configs_from_csv(CONFIG_FILE_PATH) # Pass full path

            if reloaded_configs:
                # If reload is successful, update configurations and last modified time
                device_configs = reloaded_configs
                last_modified_time = current_modified_time
                print(f"✅ Successfully reloaded {len(device_configs)} configurations.")
            else:
                # If reload fails, print a warning
                print(f"⚠️ Failed to reload configurations from {CONFIG_FILENAME}. Using previous configurations (if any).")
                # If reload fails but the file exists, force a check next cycle by resetting last_modified_time
                if current_modified_time is not None:
                    last_modified_time = 0
                # Keep the old configurations if the new load failed
                device_configs = device_configs

        # If there are no active configurations
        if not device_configs:
            print(f"No active configurations to process. Waiting for {CONFIG_FILENAME} to become valid...")
            sleep(12) # Wait longer if no configs are loaded
            continue # Skip the rest of the loop and try again

        # Loop through each device configuration
        for config_item in device_configs:
            # Fetch data and publish it for the current device
            fetch_and_publish_device_data(config_item)
            sleep(1) # Wait for 1 second between processing each device

        sleep(12) # Wait for 12 seconds before the next full cycle of checking and publishing