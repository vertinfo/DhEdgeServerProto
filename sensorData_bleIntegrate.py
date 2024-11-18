import asyncio
import json
import sqlite3
from bleak import BleakScanner, BleakClient

# Define the specific service and characteristic UUIDs
SERVICE_UUID = "00000000-0001-11e1-9ab4-0002a5d5c51b"
CHARACTERISTIC_UUIDS = [
    "00000004-0002-11e1-ac36-0002a5d5c51b"
]

# SQLite Database setup
DB_FILE = "datahoist"  # Path to your SQLite database file
buffer = ""  # Buffer to accumulate incoming data
SCAN_DURATION = 30  # Maximum scanning duration in seconds
NOTIFICATION_TIMEOUT = 30  # Time to wait for notifications before disconnecting

# Track active clients to manage connections
active_clients = set()  # Track active client addresses

# Open a single database connection
connection = sqlite3.connect(DB_FILE)
cursor = connection.cursor()

# Function to insert data into the SQLite database
def insert_data(data):
    global connection, cursor

    # Check if the table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sensor_data';")
    if cursor.fetchone() is None:
        print("Table 'sensor_data' does not exist!")
        return

    insert_query = '''
        INSERT INTO sensor_data (time, edgeId, x_dur, x_pval, x_mval, x_stdev, x_out, x_freq,
                                  y_dur, y_pval, y_mval, y_stdev, y_out, y_freq,
                                  z_dur, z_pval, z_mval, z_stdev, z_out, z_freq,
                                  vec_mag, vec_dir)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    '''

    # Extracting values from the JSON data
    values = (
        data['time'],
        data['edgeId'],
        data['x']['dur'], data['x']['pval'], data['x']['mval'], data['x']['stdev'], data['x']['out'], data['x']['freq'],
        data['y']['dur'], data['y']['pval'], data['y']['mval'], data['y']['stdev'], data['y']['out'], data['y']['freq'],
        data['z']['dur'], data['z']['pval'], data['z']['mval'], data['z']['stdev'], data['z']['out'], data['z']['freq'],
        data['vec']['mag'], data['vec']['dir']
    )

    try:
        cursor.execute(insert_query, values)
        connection.commit()
        print(f"Data inserted: {json.dumps(data, indent=2)}")
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")

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
                insert_data(json_data)  # Insert data into the database
                buffer = ""  # Clear the buffer after processing
                break  # Break the loop after successfully processing the full message
            except json.JSONDecodeError:
                # If JSON is not complete, break and wait for more data
                break

    except UnicodeDecodeError:
        print("Notification received: Decoding Error: The bytearray does not represent valid UTF-8 text.")

async def terminate_active_connections():
    """Terminate all active client connections."""
    if active_clients:
        print("Terminating active connections:")
        for address in list(active_clients):
            try:
                async with BleakClient(address) as client:
                    if client.is_connected:
                        await client.disconnect()
                        print(f"Disconnected from {address}")
            except Exception as e:
                print(f"Error disconnecting from {address}: {e}")
            finally:
                active_clients.discard(address)

async def run():
    global last_notification_time
    last_notification_time = asyncio.get_event_loop().time()  # Initialize last notification time

    max_attempts = 3  # Maximum number of connection attempts
    attempt_count = 0

    while attempt_count < max_attempts:
        print("Checking for active clients...")
        await terminate_active_connections()  # Disconnect any active clients

        print(f"Scanning for BLE devices for up to {SCAN_DURATION} seconds...")

        target_device_name = "5200001"  # Replace with your device's name
        target_device = None
        scan_start_time = asyncio.get_event_loop().time()

        # Scanning loop with timeout
        while (asyncio.get_event_loop().time() - scan_start_time) < SCAN_DURATION:
            devices = await BleakScanner.discover(timeout=1)  # Check every second

            # Check if the target device is in the list of discovered devices
            target_device = next((d for d in devices if d.name == target_device_name), None)
            if target_device:
                print(f"Found device: {target_device.name}, Address: {target_device.address}")
                break  # Exit the loop if the device is found

        # If device is not found, increment the attempt count
        if not target_device:
            attempt_count += 1
            print(f"Attempt {attempt_count}/{max_attempts} failed. Retrying...")

            if attempt_count >= max_attempts:
                print("Device not found.")
                return  # Exit after maximum attempts
            else:
                await asyncio.sleep(1)  # Short delay before retrying
                continue

        # Reset attempt count upon successful discovery
        attempt_count = 0
        target_address = target_device.address

        try:
            async with BleakClient(target_address) as client:
                print(f"Connected to {target_address}")
                active_clients.add(target_address)  # Add to active clients

                # Wait a moment to ensure the connection is stable
                await asyncio.sleep(1)

                # Discover services and characteristics
                services = await client.get_services()
                for service in services:
                    print(f"Service: {service.uuid}")
                    for char in service.characteristics:
                        print(f"  Characteristic: {char.uuid} (Properties: {char.properties})")

                # Start notifications for each characteristic with notify property
                global notification_received
                notification_received = False  # Reset the notification flag
                
                async def async_notification_handler(characteristic, data):
                    await notification_handler(characteristic, data)

                for char_uuid in CHARACTERISTIC_UUIDS:
                    await client.start_notify(char_uuid, lambda c, d: asyncio.ensure_future(async_notification_handler(c, d)))
                    print(f"Started notifications for characteristic: {char_uuid}")

                # Monitor notifications and handle timeout
                while True:
                    if not notification_received:
                        print("NO NOTIFICATIONS")
                        current_time = asyncio.get_event_loop().time()
                        if current_time - last_notification_time > NOTIFICATION_TIMEOUT:
                            print("No notifications received for a while. Reconnecting...")
                            break  # Exit to reconnect
                    notification_received = False  # Reset for the next cycle
                    await asyncio.sleep(1)

        except Exception as conn_error:
            print(f"Failed to connect to {target_address}: {conn_error}")

        finally:
            # Remove from active clients after disconnection
            print("Active Clients... Flushing", target_address)
            active_clients.discard(target_address)
            # Close the global database connection after finishing
            connection.close()
            print("Database connection closed.")


asyncio.run(run())