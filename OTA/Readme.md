# OTA - Firmware Update from RaspberryPi to STM32 Device

## Steps to be followed before executing the script:
* Open the file OTAFirmwareUpdate.py in a text editor or IDE
* Modify the BLE_DEVICE_ADDRESS1 variable to match the MAC address of the bluetooth device which we are trying to connect to
* firmware.bin file represents the firmware binary which we want to upload to the STM32 device
* New firmware file can be placed here and have to be renamed to firmware.bin

## Firmware Upgrade Steps:
* After making sure all the steps are taken care of the python script needs to be executed. python3 OTAFirmwareUpdate.py
* The script first scans for the devices that are present in the vicinity. Select the device for which you want to perform the firmware upgrade.
* You will be prompted with an option to "START THE FIRMWARE UPGRADE", if we are able to establish the connection with the device. Type "YES" and press enter when asked.
* The firmware update will happen and the data will be transmitted. Wait for the program to END.

