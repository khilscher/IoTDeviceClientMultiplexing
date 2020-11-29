using System;
using System.Text;
using Microsoft.Azure.Devices.Client; // IoT Hub Device SDK
using Microsoft.Azure.Devices; //IoT Hub Service SDK
using System.Threading.Tasks;
using Microsoft.Azure.Devices.Common.Exceptions;
using System.Collections.Generic;
using System.Linq;

namespace DeviceClientMultiplexing
{
    class Multiplex
    {

        private static RegistryManager _registryManager;
        private static string _ioTHubHostName;

        // Change these to meet your needs
        private static string _prefix = "tdevice"; // Prefix for each deviceId
        private static int _devicesToSimulate = 1000; // Number of IoT devices to create
        private static bool _pooling = true; // Enable or disable pooling (multiplexing)
        private static uint _maxPoolSize = 2; 
        private static int _sendIterations = 10; // Number of send message iterations
        private static int _delayBetweenSendInterations = 10000; // Delay between send iterations, in ms

        // Replace with you IoT Hub owner connection string
        private static string _iotHubConnString = "<your IoT Hub owner connection string>";

        public Multiplex()
        {

            _registryManager = RegistryManager.CreateFromConnectionString(_iotHubConnString);

            // Extract IoT Hub hostname from connection string
            int start = _iotHubConnString.IndexOf("=") + 1;
            int end = _iotHubConnString.IndexOf(";");
            _ioTHubHostName = _iotHubConnString.Substring(start, end - start);

        }

        public async Task Start()
        {

            // Create an array of Devices 
            Device[] devices = new Device[_devicesToSimulate];

            // Set the deviceid for each Device
            for (int i = 0; i < _devicesToSimulate; i++)
            {

                devices[i] = new Device(_prefix + i);

            }

            // Register the device using the Service API. Ordinarily this should be done using DPS.
            // Returns back the eTag and authentication key. 
            // This is being run sequentually due to IoT Hub identity operations throttling limits (100/min/unit).
            // https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-devguide-quotas-throttling
            for (int i = 0; i < _devicesToSimulate; i++)
            {

                try
                {

                    devices[i] = await _registryManager.AddDeviceAsync(new Device(devices[i].Id));
                    Console.WriteLine($"Created device {devices[i].Id}");

                }
                catch (DeviceAlreadyExistsException)
                {

                    devices[i] = await _registryManager.GetDeviceAsync(devices[i].Id);
                    Console.WriteLine($"Device {devices[i].Id} already exists");

                }

            }
            
            // Create empty array of DeviceClients
            DeviceClient[] deviceClients = new DeviceClient[_devicesToSimulate];

            // Populate the array with a DeviceClient created for each Device
            for (int i = 0; i < _devicesToSimulate; i++)
            {

                try
                {

                    var auth = new DeviceAuthenticationWithRegistrySymmetricKey(devices[i].Id, devices[i].Authentication.SymmetricKey.PrimaryKey);

                    // For IoT PnP discovery
                    ClientOptions options = new ClientOptions() { ModelId = "dtmi:company:interface;1" };

                    deviceClients[i] = DeviceClient.Create(
                        _ioTHubHostName,
                        auth,
                        new ITransportSettings[]
                        {
                        new AmqpTransportSettings(Microsoft.Azure.Devices.Client.TransportType.Amqp_Tcp_Only)
                        {
                            AmqpConnectionPoolSettings = new AmqpConnectionPoolSettings()
                            {
                                Pooling = _pooling,
                                MaxPoolSize = _maxPoolSize
                            }
                        }
                    }, options);

                }
                catch (Exception e)
                {

                    Console.WriteLine($"DeviceClient.Create Error: {e.Message}");

                }

            }

            for (int x = 0; x < _sendIterations; x++)
            {

                Console.WriteLine($"********** Send iteration {x} **********");

                
                // Send all the messages in parallel
                Parallel.ForEach(deviceClients, async (dc, pls, index) =>
                {

                    SendMessage(dc, index);

                });
                
                await Task.Delay(_delayBetweenSendInterations);

            }

            // Short delay to ensure all messages are received before deleting devices.
            await Task.Delay(5000);

            // Clean up
            Console.WriteLine("Deleting devices...");

            try
            {

                // Unregister all devices with IoT Hub using the Service API.
                // RemoveDevices2Async can only do 100 at a time :-(
                if (devices.Length > 100)
                {

                    int count = devices.Length / 100;

                    // Convert to a list because it's easier to work with.
                    List<Device> deviceList = new List<Device>(devices);

                    for (int x = 0; x < count; x++)
                    {

                        // Unregister and delete in batches of 100
                        await _registryManager.RemoveDevices2Async(deviceList.Take(100));
                        deviceList.RemoveRange(0, 100);
                        Console.WriteLine($"Devices remaining to be deleted {deviceList.Count}");

                    }

                    // Unregister any remaining devices from IoT Hub
                    if (deviceList.Count > 0)
                    {

                        await _registryManager.RemoveDevices2Async(deviceList);

                    }

                }
                else
                {

                    await _registryManager.RemoveDevices2Async(devices);

                }

            }
            catch (Exception e)
            {

                Console.WriteLine($"RemoveDevices2Async Error: {e.Message}");

            }

        }

        /// <summary>
        /// Sends a D2C message containing the deviceId in the message payload
        /// </summary>
        /// <param name="dc"></param>
        /// <param name="index"></param>
        private void SendMessage(DeviceClient dc, long index)
        {

            try
            {

                Microsoft.Azure.Devices.Client.Message msg = new Microsoft.Azure.Devices.Client.Message(Encoding.UTF8.GetBytes(_prefix + index));
                dc.SendEventAsync(msg);
                Console.WriteLine($"Sent message to {_prefix}{index}");

            }
            catch (Exception e)
            {

                Console.WriteLine($"SendMessage Error: {e.Message}");

            }

        }

    }

}
