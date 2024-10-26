import asyncio
import json
import sqlite3
from bleak import BleakScanner, BleakClient

# Define the specific service and characteristic UUIDs
SERVICE_UUID = "00000000-0001-11e1-9ab4-0002a5d5c51b"
CHARACTERISTIC_UUID = "00000005-0002-11e1-ac36-0002a5d5c51b"

# SQLite Database setup
DB_FILE = "datahoist"  # Path to your SQLite database file
# DB_FILE = "sqlite:///C:/Users/umesh/Desktop/datahoist"
buffer = ""  # Buffer to accumulate incoming data

# Function to insert data into the SQLite database
def insert_data(data):
    connection = sqlite3.connect(DB_FILE)
    cursor = connection.cursor()
    
    # Check if the table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sensor_data';")
    if cursor.fetchone() is None:
        print("Table 'sensor_data' does not exist!")
        connection.close()
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
    finally:
        connection.close()


async def notification_handler(characteristic, data):
    """Callback function to handle notifications from the characteristic."""
    global buffer
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

async def run():
    print("Scanning for BLE devices...")
    devices = await BleakScanner.discover()
    
    for device in devices:
        print(f"Device: {device.name}, Address: {device.address}")
    
    target_device_name = "PM1V240"  # Replace with your device's name
    target_device = next((d for d in devices if d.name == target_device_name), None)

    if target_device is None:
        print(f"Device '{target_device_name}' not found.")
        return

    target_address = target_device.address
    
    try:
        async with BleakClient(target_address) as client:
            print(f"Connected to {target_address}")

            # Wait a moment to ensure the connection is stable
            await asyncio.sleep(1)

            # Discover services and characteristics
            services = await client.get_services()
            for service in services:
                print(f"Service: {service.uuid}")
                for char in service.characteristics:
                    print(f"  Characteristic: {char.uuid} (Properties: {char.properties})")

            # Start notifications from the specified characteristic
            await client.start_notify(CHARACTERISTIC_UUID, notification_handler)
            print(f"Started notifications for characteristic: {CHARACTERISTIC_UUID}")

            # Keep the connection open and wait for notifications
            while True:
                await asyncio.sleep(1)  # Adjust the delay as needed

    except Exception as conn_error:
        print(f"Failed to connect to {target_address}: {conn_error}")

asyncio.run(run())
