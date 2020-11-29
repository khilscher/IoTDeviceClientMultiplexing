using Microsoft.Azure.Devices;
using System;

namespace DeviceClientMultiplexing
{
    class Program
    {

        static void Main(string[] args)
        {

            try
            {

                Multiplex multiplexTest = new Multiplex();
                multiplexTest.Start().GetAwaiter().GetResult();

            }
            catch (Exception e)
            {

                Console.WriteLine($"{e.Message}");

            }

            Console.WriteLine("Finished!");

        }

    }

}
