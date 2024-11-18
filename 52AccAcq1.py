import asyncio
import json
import sqlite3
from bleak import BleakScanner, BleakClient
import logging
from datetime import datetime, timedelta

# Custom Formatter to apply +5:30 offset
class OffsetFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        # Apply +5:30 offset
        local_dt = datetime.utcnow() + timedelta(hours=5, minutes=30)
        if datefmt:
            return local_dt.strftime(datefmt)
        else:
            return local_dt.isoformat()

# Configure logging with the custom formatter
handler = logging.FileHandler('accelerometer_2_acquisition.log')
handler.setFormatter(OffsetFormatter(
    fmt='%(asctime)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S"
))

# Set up logger and add handler
logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

logger.info("This log entry should show time with +5:30 offset.")

# Define the specific characteristic UUID for Accelerometer data
CHARACTERISTIC_UUID = "00000005-0002-11e1-ac36-0002a5d5c51b"  
MAX_NOTIFICATION_ATTEMPTS = 0  # Max attempts to receive notifications within the timeout period

# SQLite Database setup
DB_FILE = "datahoist_v1"  # Path to your SQLite database file
buffer = ""  # Buffer to accumulate incoming data
SCAN_DURATION = 30  # Maximum scanning duration in seconds
NOTIFICATION_TIMEOUT = 30  # Time to wait for notifications before disconnecting
notification_received = 0 
# Track active clients to manage connections
active_clients = set()  # Track active client addresses
global notification_attempts 

# Open a single database connection
connection = sqlite3.connect(DB_FILE)
cursor = connection.cursor()

# Define table name as a variable
TABLE_NAME = "accelerometer_data"  # Change table name to reflect accelerometer data

# Function to insert data into the SQLite database
def insert_data(data):
    global connection, cursor

    # Ensure the table exists; create it if it doesn't
    create_table_query = f'''
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            time TEXT,
            edgeId TEXT,
            x_accel REAL,
            y_accel REAL,
            z_accel REAL,
            vec_mag REAL,
            vec_dir REAL
        );
    '''
    cursor.execute(create_table_query)

    # Insert data into the table
    insert_query = f'''
        INSERT INTO {TABLE_NAME} (time, edgeId, x_accel, y_accel, z_accel, vec_mag, vec_dir)
        VALUES (?, ?, ?, ?, ?, ?, ?);
    '''

    # Using .get() to avoid KeyError and setting default values if keys are missing
    values = (
        data.get('time', None),
        data.get('edgeId', None),
        data.get('x', {}).get('accel', None),  # Adjust to accelerometer-specific data
        data.get('y', {}).get('accel', None),
        data.get('z', {}).get('accel', None),
        data.get('vec', {}).get('mag', None),
        data.get('vec', {}).get('dir', None)
    )

    try:
        cursor.execute(insert_query, values)
        connection.commit()
        logging.debug(f"Data inserted: {json.dumps(data, indent=2)}")
    except sqlite3.Error as e:
        logging.error(f"SQLite error: {e}")        
notification_received = False

async def notification_handler(characteristic, data):
    """Callback function to handle notifications from the characteristic."""
    global buffer, notification_received
    notification_received = True  # Indicate that a notification has been received

    try:
        # Decode the received data
        raw_text = data.decode('utf-8')
        buffer += raw_text  # Append the new data to the buffer

        # Check if we have a complete JSON object
        while True:
            try:
                # Attempt to load the JSON data
                json_data = json.loads(buffer)
                logging.debug("JSON DATA", json_data)
                print('Data received', json_data)
                insert_data(json_data)  # Insert data into the database
                logging.info("Data Received", json_data)
                buffer = ""  # Clear the buffer after processing

                break  # Break the loop after successfully processing the full message
            except json.JSONDecodeError:
                # If JSON is not complete, break and wait for more data
                break

    except UnicodeDecodeError:
        logging.error("Notification received: Decoding Error: The bytearray does not represent valid UTF-8 text.")

count = 0
async def check_for_notification():
    """
    Coroutine to wait for a notification to be received.
    This will loop and wait until `notification_received` is True.
    """
    global notification_received
    global count
    count+= 1
    # while not notification_received:
    await asyncio.sleep(31)  # Small delay to avoid busy waiting
    notification_received = False  # Reset for the next wait

async def terminate_active_connections():
    """Terminate all active client connections."""
    if active_clients:
        logging.debug("Terminating active connections:")
        for address in list(active_clients):
            try:
                async with BleakClient(address) as client:
                    if client.is_connected:
                        await client.disconnect()
                        logging.debug(f"Disconnected from {address}")
                        print("Disconnected from", address)
            except Exception as e:
                logging.error(f"Error disconnecting from {address}: {e}")
            finally:
                active_clients.discard(address)

async def run():
    max_attempts = 3  # Maximum number of connection attempts
    attempt_count = 0

    while attempt_count < max_attempts:
        logging.debug("Checking for active clients...")
        print("Checking for active clients...")

        await terminate_active_connections()  # Disconnect any active clients

        target_device_name = "5200002"
        target_device = None
        scan_start_time = asyncio.get_event_loop().time()

        # Single scan for device
        while (asyncio.get_event_loop().time() - scan_start_time) < SCAN_DURATION:
            devices = await BleakScanner.discover(timeout=1)  # Check every second
            target_device = next((d for d in devices if d.name == target_device_name), None)
            if target_device:
                logging.info(f"Found device: {target_device.name}, Address: {target_device.address}")
                print(f"Found device: {target_device.name}, Address: {target_device.address}")
                break

        if not target_device:
            attempt_count += 1
            logging.debug(f"Attempt {attempt_count}/{max_attempts} failed. Retrying...")
            print(f"Attempt {attempt_count}/{max_attempts} failed. Retrying...")
            if attempt_count >= max_attempts:
                logging.debug("Device not found.")
                return  # Exit if max attempts are reached without finding the device
            await asyncio.sleep(1)  # Small delay before retry
            continue

        target_address = target_device.address
        attempt_count = 0  # Reset attempt count after successful discovery

        # Try connecting up to 3 times
        try:
            async with BleakClient(target_address) as client:
                logging.debug(f"Connected to {target_address}")
                print(f"Connected to {target_address}")
                active_clients.add(target_address)
                await asyncio.sleep(1)

                logging.debug(f"Started notifications for characteristic: {CHARACTERISTIC_UUID}")
                print(f"Started notifications for characteristic: {CHARACTERISTIC_UUID}")
                
                # Start notifications
                await client.start_notify(CHARACTERISTIC_UUID, notification_handler)
                notification_received = False  # Reset before notification loop

                while True:
                    notification_attempts = 0 
                    # Wait for notifications with a 30-second timeout
                    try:
                        await asyncio.wait_for(check_for_notification(), timeout=30)
                        notification_attempts = 0  # Reset attempts on success

                    except asyncio.TimeoutError:
                        logging.error("No notifications received within 30 seconds. Reconnecting...")
                        print("No notifications received within 30 seconds. Reconnecting...")
                        notification_attempts += 1
                        if notification_attempts >= MAX_NOTIFICATION_ATTEMPTS:
                            logging.error("Max notification attempts reached. Reconnecting...")
                            break  # Exit loop to reconnect

                    if notification_received:
                        notification_received = False  # Reset after processing
                        logging.debug("Notification received, processing data.")
                        continue  # Continue receiving more notifications

        except Exception as conn_error:
            logging.error(f"Failed to connect to {target_address}: {conn_error}")
            print(f"Failed to connect to {target_address}: {conn_error}")
            attempt_count += 1
            if attempt_count >= max_attempts:
                print(f"Reached maximum attempts ({max_attempts}) without successful connection.")
                break  # Exit if all connection attempts fail

        finally:
            logging.debug("Cleaning up active client: %s", target_address)
            active_clients.discard(target_address)
            await asyncio.sleep(1)  # Delay before reattempting


asyncio.run(run())
