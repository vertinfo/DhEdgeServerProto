import asyncio
import json
import sqlite3
import logging
from datetime import datetime, timedelta
from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device import X509
from bleak import BleakScanner, BleakClient
import os
import time

#Load Configuration
def load_config():
    try:
        with open('Datahoist/V1_Scripts/Azure_iot_integration/Final_scripts/iot_config.json', 'r') as config_file:
            return json.load(config_file)
    except FileNotFoundError:
        print("Configuration file not found at the specified path.")
        logging.info("Configuration file not found at the specified path.")
        return None

# Read the configuration values
config = load_config()

if config is not None:  # Check if the configuration was loaded successfully
    IOT_HUB_HOSTNAME = config.get('hub_hostname')
    CERT_PATH = config.get('cert_file')
    KEY_PATH = config.get('key_file')
else:
    # Handle the case where the configuration file does not exist or cannot be read
    print("Unable to load configuration.")
    logging.info("Unable to load configuration")


# Function to convert Unix timestamp in milliseconds to hh:mm:ss
def convert_unix_timestamp_to_time(unix_timestamp_ms):
    # Convert milliseconds to seconds
    seconds = unix_timestamp_ms / 1000
    # Convert seconds to hh:mm:ss format
    return time.strftime('%H:%M:%S', time.gmtime(seconds))

# Logging Configuration
class OffsetFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        local_dt = datetime.utcnow() + timedelta(hours=5, minutes=30)
        if datefmt:
            return local_dt.strftime(datefmt)
        else:
            return local_dt.isoformat()
        
log_file_path = 'sensor_data_to_azure.log'
log_directory = os.path.dirname(log_file_path)

if not os.path.exists(log_directory) and log_directory:
    os.makedirs(log_directory)        

# Set up the file handler and apply the formatter
handler = logging.FileHandler(log_file_path)
handler.setFormatter(OffsetFormatter(
    fmt='%(asctime)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S"
))

# Set up the logger
logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

logger.info("Logging initialized with +5:30 offset.")
print("Logging initialized. Logs will be saved in sensor_data_to_azure.log")

# Constants
CHARACTERISTIC_UUID = "00000005-0002-11e1-ac36-0002a5d5c51b"
DB_FILE = "SOTL52"
TABLE_NAME = "Sensor_data"
SCAN_DURATION = 30
MAX_NOTIFICATION_ATTEMPTS = 3

# Establish a connection to the SQLite database
connection = sqlite3.connect('sensor_data.db')
cursor = connection.cursor()

def create_table():
    """
    Create the sensor_data table if it doesn't exist.
    """
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS sensor_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        time INTEGER,
        edgeId TEXT,
        temp INTEGER,
        acc_x_pval REAL, acc_x_mval REAL, acc_x_stdev REAL, acc_x_freq INTEGER,
        acc_y_pval REAL, acc_y_mval REAL, acc_y_stdev REAL, acc_y_freq INTEGER,
        acc_z_pval REAL, acc_z_mval REAL, acc_z_stdev REAL, acc_z_freq INTEGER,
        acc_vec_mag REAL, acc_vec_dir REAL,
        mag_x_pval REAL, mag_x_mval REAL, mag_x_stdev REAL, mag_x_freq INTEGER,
        mag_y_pval REAL, mag_y_mval REAL, mag_y_stdev REAL, mag_y_freq INTEGER,
        mag_z_pval REAL, mag_z_mval REAL, mag_z_stdev REAL, mag_z_freq INTEGER,
        mag_vec_mag REAL, mag_vec_dir REAL,
        data_sent INTEGER DEFAULT 0
    );
    '''
    cursor.execute(create_table_query)
    connection.commit()
    print("Table checked or created successfully.")
    logging.info("Table checked or created successfully.")

def add_missing_columns():
    """
    Add missing columns to the sensor_data table dynamically.
    """
    # Expected columns
    expected_columns = [
        "time","id", "edgeId", "temp",
        "acc_x_pval", "acc_x_mval", "acc_x_stdev", "acc_x_freq",
        "acc_y_pval", "acc_y_mval", "acc_y_stdev", "acc_y_freq",
        "acc_z_pval", "acc_z_mval", "acc_z_stdev", "acc_z_freq",
        "acc_vec_mag", "acc_vec_dir",
        "mag_x_pval", "mag_x_mval", "mag_x_stdev", "mag_x_freq",
        "mag_y_pval", "mag_y_mval", "mag_y_stdev", "mag_y_freq",
        "mag_z_pval", "mag_z_mval", "mag_z_stdev", "mag_z_freq",
        "mag_vec_mag", "mag_vec_dir",
        "data_sent"
    ]

    # Get existing columns in the table
    cursor.execute("PRAGMA table_info(sensor_data);")
    existing_columns = [row[1] for row in cursor.fetchall()]

    # Add any missing columns
    for column in expected_columns:
        if column not in existing_columns:
            column_type = "REAL" if "val" in column or "mag" in column else "TEXT" if column == "temp" or column == "edgeId" else "INTEGER"
            alter_table_query = f"ALTER TABLE sensor_data ADD COLUMN {column} {column_type};"
            cursor.execute(alter_table_query)
            print(f"Added missing column: {column}")
            logging.info(f"Added missing column: {column}")

    connection.commit()

def insert_data(data):
    """
    Insert sensor data (accelerometer or magnetometer) into the database.
    """
    time = data.get('time')  # Expecting Unix timestamp in milliseconds
    formatted_time = convert_unix_timestamp_to_time(time)  # Convert timestamp to hh:mm:ss    
    edgeId = data.get('edgeId')
    temp = data.get('temp')
    acc_data = data.get('acc', {})
    mag_data = data.get('mag', {})

    # Accelerometer values
    acc_values = (
        acc_data.get('x', {}).get('pval'),
        acc_data.get('x', {}).get('mval'),
        acc_data.get('x', {}).get('stdev'),
        acc_data.get('x', {}).get('freq'),
        acc_data.get('y', {}).get('pval'),
        acc_data.get('y', {}).get('mval'),
        acc_data.get('y', {}).get('stdev'),
        acc_data.get('y', {}).get('freq'),
        acc_data.get('z', {}).get('pval'),
        acc_data.get('z', {}).get('mval'),
        acc_data.get('z', {}).get('stdev'),
        acc_data.get('z', {}).get('freq'),
        acc_data.get('vec', {}).get('mag'),
        acc_data.get('vec', {}).get('dir'),
    )

    # Magnetometer values
    mag_values = (
        mag_data.get('x', {}).get('pval'),
        mag_data.get('x', {}).get('mval'),
        mag_data.get('x', {}).get('stdev'),
        mag_data.get('x', {}).get('freq'),
        mag_data.get('y', {}).get('pval'),
        mag_data.get('y', {}).get('mval'),
        mag_data.get('y', {}).get('stdev'),
        mag_data.get('y', {}).get('freq'),
        mag_data.get('z', {}).get('pval'),
        mag_data.get('z', {}).get('mval'),
        mag_data.get('z', {}).get('stdev'),
        mag_data.get('z', {}).get('freq'),
        mag_data.get('vec', {}).get('mag'),
        mag_data.get('vec', {}).get('dir'),
    )

    # Combine all values
    values = (
        formatted_time,
        edgeId,
        temp,
        *acc_values,
        *mag_values,
        0  # data_sent
    )

    # SQL Insert Statement
    insert_query = '''
        INSERT INTO sensor_data (
            time, edgeId, temp,
            acc_x_pval, acc_x_mval, acc_x_stdev, acc_x_freq,
            acc_y_pval, acc_y_mval, acc_y_stdev, acc_y_freq,
            acc_z_pval, acc_z_mval, acc_z_stdev, acc_z_freq,
            acc_vec_mag, acc_vec_dir,
            mag_x_pval, mag_x_mval, mag_x_stdev, mag_x_freq,
            mag_y_pval, mag_y_mval, mag_y_stdev, mag_y_freq,
            mag_z_pval, mag_z_mval, mag_z_stdev, mag_z_freq,
            mag_vec_mag, mag_vec_dir,
            data_sent
        ) VALUES (
            ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?,
            ?
        );
    '''

    try:
        cursor.execute(insert_query, values)
        connection.commit()
        logging.info(f"Data inserted: {json.dumps(data, indent=2)}")
        print(f"Data successfully inserted into SQLite database.")
    except sqlite3.Error as e:
        logging.error(f"SQLite error while inserting data: {e}")
        print(f"SQLite error while inserting data: {e}")

# Ensure the table and columns exist
create_table()
add_missing_columns()

def get_unsent_data():
    select_query = f"SELECT * FROM {TABLE_NAME} WHERE data_sent = 0;"
    cursor.execute(select_query)
    return cursor.fetchall()

# Azure IoT Hub Interaction
async def check_if_device_exist_in_azure_hub(device_name):
    device_id = "dh00001001"
    try:
        x509 = X509(
            cert_file=CERT_PATH,
            key_file=KEY_PATH,
            pass_phrase=None
        )
        device_client = IoTHubDeviceClient.create_from_x509_certificate(
            hostname=IOT_HUB_HOSTNAME,
            x509=x509,
            device_id=device_id,
        )
        await device_client.connect()
        logging.info(f"Device {device_id} successfully authenticated with IoT Hub.")
        print(f"Device '{device_id}' authenticated with IoT Hub.")
        return device_client
    except Exception as e:
        logging.error(f"Failed to authenticate device {device_name}: {e}")
        print(f"Authentication failed for device '{device_name}': {e}")
        return None

async def send_to_azure_hub(device_client, data):
    try:
        json_message = json.dumps(data)
        await device_client.send_message(json_message)
        logging.info(f"Sent message to Azure IoT Hub: {json_message}")
        print(f"Data sent to Azure IoT Hub: {json_message}")
    except Exception as e:
        logging.error(f"Failed to send message to Azure IoT Hub: {e}")
        print(f"Failed to send data to Azure IoT Hub: {e}")

async def send_unsent_data_to_azure(device_client):
    """
    Fetch unsent data, prepare it for Azure IoT Hub, send in bulk, and mark records as sent.
    """
    unsent_data = get_unsent_data()  # Retrieve unsent records from the database

    if not unsent_data:
        logging.info("No unsent data to send to Azure IoT Hub.")
        print("No unsent data to send.")
        return

    # Initialize bulk data payload and record tracking
    bulk_data = []
    record_ids = []

    for record in unsent_data:
        try:
            # Unpack record details
            record_id, time, edgeId, temp, acc_data, mag_data, data_sent, *extra_fields = record

            # Prepare accelerometer and magnetometer values
            acc_values = (
                acc_data.get('x', {}).get('pval', None),
                acc_data.get('x', {}).get('mval', None),
                acc_data.get('x', {}).get('stdev', None),
                acc_data.get('x', {}).get('freq', None),
                acc_data.get('y', {}).get('pval', None),
                acc_data.get('y', {}).get('mval', None),
                acc_data.get('y', {}).get('stdev', None),
                acc_data.get('y', {}).get('freq', None),
                acc_data.get('z', {}).get('pval', None),
                acc_data.get('z', {}).get('mval', None),
                acc_data.get('z', {}).get('stdev', None),
                acc_data.get('z', {}).get('freq', None),
                acc_data.get('vec', {}).get('mag', None),
                acc_data.get('vec', {}).get('dir', None),
            ) if acc_data else None

            mag_values = (
                mag_data.get('x', {}).get('pval', None),
                mag_data.get('x', {}).get('mval', None),
                mag_data.get('x', {}).get('stdev', None),
                mag_data.get('x', {}).get('freq', None),
                mag_data.get('y', {}).get('pval', None),
                mag_data.get('y', {}).get('mval', None),
                mag_data.get('y', {}).get('stdev', None),
                mag_data.get('y', {}).get('freq', None),
                mag_data.get('z', {}).get('pval', None),
                mag_data.get('z', {}).get('mval', None),
                mag_data.get('z', {}).get('stdev', None),
                mag_data.get('z', {}).get('freq', None),
                mag_data.get('vec', {}).get('mag', None),
                mag_data.get('vec', {}).get('dir', None),
            ) if mag_data else None

            # Add to bulk data
            bulk_data.append({
                "time": time,
                "edgeId": edgeId,
                "temp": temp,
                "acc": acc_values,
                "mag": mag_values
            })

            # Track record ID for updating status
            record_ids.append(record_id)

        except Exception as e:
            logging.error(f"Error processing record {record}: {e}")
            print(f"Error processing record {record}: {e}")
            continue

    try:
        # Convert data to JSON format and send to Azure IoT Hub
        json_message = json.dumps(bulk_data)
        await device_client.send_message(json_message)
        logging.info(f"Successfully sent bulk data to Azure IoT Hub: {json_message}")
        print(f"Sent bulk data to Azure IoT Hub: {json_message}")

        # Update database to mark records as sent
        update_query = f"UPDATE {TABLE_NAME} SET data_sent = 1 WHERE id = ?;"
        cursor.executemany(update_query, [(record_id,) for record_id in record_ids])
        connection.commit()
        logging.info(f"Marked {len(record_ids)} records as sent in the database.")
        print(f"Marked {len(record_ids)} records as sent.")
    except Exception as e:
        logging.error(f"Failed to send bulk data to Azure IoT Hub: {e}")
        print(f"Failed to send bulk data to Azure IoT Hub: {e}")

# BLE Interaction
async def scan_for_device(target_device_name, retries=3, initial_delay=1):
    delay = initial_delay
    for attempt in range(retries):
        devices = await BleakScanner.discover(timeout=SCAN_DURATION)
        target_device = next((d for d in devices if d.name == target_device_name), None)
        if target_device:
            return target_device
        logging.warning(f"Retry {attempt + 1}: Device not found. Retrying in {delay} seconds...")
        await asyncio.sleep(delay)
        delay *= 2
    return None

async def run():
    attempt_count = 0
    while attempt_count < MAX_NOTIFICATION_ATTEMPTS:
        target_device_name = "SOTL52"
        target_device = await scan_for_device(target_device_name)
        
        if not target_device:
            attempt_count += 1
            logging.warning(f"Attempt {attempt_count}/{MAX_NOTIFICATION_ATTEMPTS} failed. Retrying...")
            print(f"Attempt {attempt_count}/{MAX_NOTIFICATION_ATTEMPTS} failed. Retrying...")
            continue

        device_client = await check_if_device_exist_in_azure_hub(target_device_name)
        notification_buffer = ""

        try:
            async with BleakClient(target_device.address) as client:
                logging.info(f"Connected to BLE Device: {target_device.address}")
                print(f"Connected to BLE Device: {target_device.address}")

                import re

                async def notification_handler(characteristic, data):
                    nonlocal notification_buffer
                    raw_text = data.decode('utf-8')
                    logging.debug(f"Raw notification fragment: {raw_text}")
                    notification_buffer += raw_text

                    # Define the regex pattern to match any number after `"dir":`
                    pattern = r'"dir":\s*(-?\d*\.?\d+)\s*}}'

                    try:
                        while notification_buffer:
                            json_data, end_index = json.JSONDecoder().raw_decode(notification_buffer)
                            notification_buffer = notification_buffer[end_index:].lstrip()

                            # Check if the buffer contains the pattern and clear the buffer if found
                            if re.search(pattern, raw_text):
                                logging.debug(f"Pattern matched in buffer, clearing it.")
                                notification_buffer = ""  # Clear the buffer when the pattern is found

                            # Insert the data into the database
                            insert_data(json_data)

                            # Clear the buffer after inserting the data to prepare for the next chunk
                            notification_buffer = ""

                    except json.JSONDecodeError:
                        logging.debug("JSON not fully received, waiting for more data.")

                await client.start_notify(CHARACTERISTIC_UUID, notification_handler)
                logging.info(f"Started notifications for {CHARACTERISTIC_UUID}")
                print(f"Started notifications for {CHARACTERISTIC_UUID}")

                while True:
                    await asyncio.sleep(180)  # 3-minute interval
                    if device_client:
                        await send_unsent_data_to_azure(device_client)
        except Exception as e:
            logging.error(f"BLE Connection error: {e}")
            print(f"BLE Connection error: {e}")
            attempt_count += 1

loop = asyncio.get_event_loop() # if event loop does not exist creates one
loop.run_until_complete(run()) # get the awaitable func and run until it is complete
