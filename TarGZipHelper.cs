using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace TarGZipSample
{
    public static class TarGZipHelper
    {
        static DateTime EpochTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);

        public static async Task ExtractTarGzip(string filename, string outputDir)
        {
            using (var stream = File.OpenRead(filename))
            {
                await ExtractTarGzip(stream, outputDir);
            }
        }

        public static async Task ExtractTarGzip(Stream stream, string outputDir)
        {
            using (var gzip = new GZipStream(stream, CompressionMode.Decompress))
            {
                await ExtractTar(gzip, outputDir);
            }
        }

        public static async Task ExtractTar(string filename, string outputDir)
        {
            using (var stream = File.OpenRead(filename))
            {
                await ExtractTar(stream, outputDir);
            }
        }

        public static async Task ExtractTar(Stream stream, string outputDir)
        {
            var buffer = new BufferedStream(stream);
            string longName = null;
            while (true)
            {
                // offset 0, len 100, File name
                var name = await buffer.ReadAsStringAsync(100, calculateChecksum: true);

                // end of file
                if (string.IsNullOrWhiteSpace(name))
                {
                    break;
                }

                if (longName != null)
                {
                    // previous entry contains long filename
                    // use that instead
                    name = longName;
                    longName = null;
                }

                // offset 100, len 8, File mode
                // offset 108, len 8, Owner's numeric user ID
                // offset 116, len 8, Group's numeric user ID
                await buffer.SkipAsync(8 * 3);

                // offset 124, len 12, File size in bytes (octal base)
                var size = await buffer.ReadAsOctalAsync(12);

                // offset 136, len 12, Last modification time in numeric Unix time format (octal)
                var dt = EpochTime.AddSeconds(await buffer.ReadAsOctalAsync(12));

                // offset 148, len 8, Checksum for header record
                var checksum = await buffer.ReadAsOctalAsync(8);
                if (checksum != buffer.HeaderChecksum)
                {
                    throw new InvalidOperationException("Mismatch tar header checksum!");
                }

                // offset 156, len 1, Link indicator(file type)
                // - '0' or (ASCII NUL)	Normal file
                // - '5'	Directory
                char fileType = await buffer.ReadAsCharAsync();

                // offset 157, len 100, Name of linked file
                await buffer.SkipAsync(100);

                // offset 257, len 6, UStar indicator "ustar" then NUL
                var magic = await buffer.ReadAsStringAsync(6);
                if (magic == "ustar" || magic == "ustar ")
                {
                    await buffer.SkipAsync(82);

                    // offset 339, len 155, prefix of fileName
                    var prefix = await buffer.ReadAsStringAsync(155);
                    if (!string.IsNullOrEmpty(prefix))
                    {
                        name = string.Format("{0}/{1}", prefix, name);
                    }
                }

                // align to 512 block
                await buffer.Align(512);

                // file type: directory
                if (fileType == '5')
                {
                    if (size != 0)
                    {
                        throw new InvalidOperationException("Directory size must be zero!");
                    }

                    var path = Path.Combine(outputDir, name.Replace('/', '\\').Trim('\\'));
                    if (!Directory.Exists(path))
                    {
                        Directory.CreateDirectory(path).LastWriteTimeUtc = dt;
                    }

                    Console.WriteLine("{0}, {1}, {2}, {3}, {4}, {5}, {6}",
                        name,
                        fileType,
                        checksum,
                        dt,
                        size,
                        magic,
                        "2jmj7l5rSw0yVb/vlWAYkK/YBwk=");

                    continue;
                }

                // file type: symlink (not supported, just skip)
                if (fileType == '1' || fileType == '2')
                {
                    Console.WriteLine("{0}, {1}, {2}, {3}, {4}, {5}, {6}",
                        name,
                        fileType,
                        checksum,
                        dt,
                        size,
                        magic,
                        "2jmj7l5rSw0yVb/vlWAYkK/YBwk=");

                    continue;
                }

                // file type: longlink - the entry contains long file name
                if (fileType == 'L')
                {
                    var strb = new StringBuilder();
                    for (int i = 0; i < size; ++i)
                    {
                        var ch = await buffer.ReadAsCharAsync();
                        if (ch == 0)
                        {
                            break;
                        }

                        strb.Append(ch);
                    }

                    if (!string.IsNullOrEmpty(longName))
                    {
                        throw new InvalidOperationException(string.Format("longName '{0}' must be null", longName));
                    }

                    longName = strb.ToString();

                    // align to 512 block
                    await buffer.Align(512);

                    continue;
                }

                if (fileType != '0' && fileType != 0)
                {
                    await buffer.SkipAsync(size);

                    // align to 512 block
                    await buffer.Align(512);

                    continue;
                }

                FileInfo fileInfo = null;
                Exception exception = null;
                try
                {
                    fileInfo = new FileInfo(Path.Combine(outputDir, name.Replace('/', '\\').Trim('\\')));
                    if (!fileInfo.Directory.Exists)
                    {
                        fileInfo.Directory.Create();
                    }
                    else
                    {
                        if (fileInfo.Exists)
                        {
                            fileInfo.Delete();
                        }
                    }
                }
                catch (Exception ex)
                {
                    exception = ex;
                }

                if (exception != null)
                {
                    Console.WriteLine("{0}, {1}, {2}, {3}, {4}, {5}, {6}",
                        name,
                        fileType,
                        checksum,
                        dt,
                        size,
                        magic,
                        exception.Message);

                    await buffer.SkipAsync(size);

                    // align to 512 block
                    await buffer.Align(512);

                    continue;
                }

                using (var fs = fileInfo.OpenWrite())
                {
                    await buffer.WriteAsync(fs, size);
                }

                fileInfo.LastWriteTimeUtc = dt;

                using (var str = fileInfo.OpenRead())
                {
                    Console.WriteLine("{0}, {1}, {2}, {3}, {4}, {5}, {6}",
                        name,
                        fileType,
                        checksum,
                        fileInfo.LastWriteTimeUtc,
                        size,
                        magic,
                        Convert.ToBase64String(GetSHA1Hash(str)));
                }

                // align to 512 block
                await buffer.Align(512);
            }
        }

        static byte[] GetSHA1Hash(Stream stream)
        {
            using (var sha1 = new SHA1Managed())
            {
                return sha1.ComputeHash(stream);
            }
        }

        public class BufferedStream
        {
            private long _position;
            private int _localRead;
            private int _localPosition;
            private int _headerChecksum;
            private byte[] _buffer;
            private Stream _stream;

            public BufferedStream(Stream stream)
            {
                _stream = stream;
                _position = 0;
                _localRead = 0;
                _localPosition = 0;
                _buffer = new byte[4096];
            }

            public int HeaderChecksum
            {
                get { return _headerChecksum; }
            }

            public long Position
            {
                get { return _position; }
            }

            public async Task<char> ReadAsCharAsync()
            {
                await FillBuffer();

                Debug.Assert(1 <= (_localRead - _localPosition));
                _position++;
                return (char)_buffer[_localPosition++];
            }

            public async Task<string> ReadAsStringAsync(int length, bool calculateChecksum = false)
            {
                await FillBuffer();

                Debug.Assert(length <= (_localRead - _localPosition));

                var result = new StringBuilder();
                for (int i = _localPosition; i < (_localPosition + length) && _buffer[i] != 0; ++i)
                {
                    result.Append((char)_buffer[i]);
                }

                if (calculateChecksum)
                {
                    _headerChecksum = CalculateHeaderChecksum();
                }

                _position += length;
                _localPosition += length;
                return result.ToString();
            }

            public async Task<long> ReadAsOctalAsync(int length)
            {
                await FillBuffer();

                Debug.Assert(length <= (_localRead - _localPosition));

                long result = 0;
                bool prePadding = true;
                for (int i = _localPosition; i < (_localPosition + length) && _buffer[i] != 0; ++i)
                {
                    if (_buffer[i] == ' ' || _buffer[i] == '0')
                    {
                        if (prePadding)
                        {
                            continue;
                        }

                        if (_buffer[i] == ' ')
                        {
                            break;
                        }
                    }

                    prePadding = false;

                    result <<= 3;
                    result += _buffer[i] - '0';
                }

                _position += length;
                _localPosition += length;
                return result;
            }

            public async Task SkipAsync(long length)
            {
                var remaining = length;
                while (remaining > 0)
                {
                    await FillBuffer();

                    if (remaining <= (_localRead - _localPosition))
                    {
                        _position += remaining;
                        _localPosition += (int)remaining;
                        break;
                    }

                    remaining -= (_localRead - _localPosition);
                    _position += (_localRead - _localPosition);
                    _localPosition = _localRead;
                }
            }

            public async Task Align(int blockSize = 512)
            {
                var skip = (int)(_position % blockSize);
                if (skip > 0)
                {
                    await SkipAsync(blockSize - skip);
                }
            }

            public int CalculateHeaderChecksum()
            {
                var checksum = 0;
                for (int i = 0; i < 512; i++)
                {
                    // offset 148, len 8, Checksum for header record
                    if (i >= 148 && i < 156)
                    {
                        checksum += ' ';
                    }
                    else
                    {
                        checksum += _buffer[_localPosition + i];
                    }
                }

                return checksum;
            }

            public async Task WriteAsync(Stream stream, long length)
            {
                var remaining = length;
                while (remaining > 0)
                {
                    await FillBuffer();

                    var read = _localRead - _localPosition;
                    var toWrite = (int)(remaining > read ? read : remaining);
                    await stream.WriteAsync(_buffer, _localPosition, toWrite);

                    remaining -= toWrite;
                    _position += toWrite;
                    _localPosition += toWrite;
                }
            }

            private async Task FillBuffer()
            {
                if (_localPosition >= _localRead)
                {
                    _localPosition -= _localRead;

                    _localRead = 0;
                    while (_localRead < _buffer.Length)
                    {
                        var read = await _stream.ReadAsync(_buffer, _localRead, _buffer.Length - _localRead);
                        if (read == 0)
                        {
                            break;
                        }

                        _localRead += read;
                    }
                }
            }
        }
    }
}
