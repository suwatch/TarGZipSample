using System;
using System.IO;

namespace TarGZipSample
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                foreach (var file in Directory.GetFiles(@"c:\temp\benchmark-files"))
                {
                    Console.WriteLine(file);
                    TarGZipHelper.ExtractTarGzip(file, @"c:\temp\targz").Wait();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
    }
}
