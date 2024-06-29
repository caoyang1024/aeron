using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Adaptive.Aeron;
using Adaptive.Agrona.Concurrent;
using Adaptive.Archiver;
using Adaptive.Archiver.Codecs;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AeronPublisher;

public class Worker(ILogger<Worker> logger) : BackgroundService
{
    private const string Channel = "aeron:ipc";
    private const int StreamId = 10;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var ctx = new Aeron.Context().AeronDirectoryName("/aeron-ipc");

        using (var aeron = Aeron.Connect(ctx))
        using (var publication = aeron.AddPublication(Channel, StreamId))
        {
            var archiveCtx = new AeronArchive.Context().ControlRequestChannel("aeron:ipc");
            using (var archive = AeronArchive.Connect(archiveCtx))
            {
                long recordingId = archive.StartRecording(Channel, StreamId, SourceLocation.LOCAL);
                Console.WriteLine($"Started recording with ID: {recordingId}");

                await ProduceMessages(publication);

                archive.StopRecording(Channel, StreamId);
                Console.WriteLine($"Stopped recording with ID: {recordingId}");
            }
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
            }
            await Task.Delay(1000, stoppingToken);
        }
    }

    private static async Task ProduceMessages(Publication publication)
    {
        for (int i = 0; i < 1000; i++)
        {
            string message = "Message-" + i;
            byte[] messageBytes = Encoding.UTF8.GetBytes(message);
            var buffer = new UnsafeBuffer(messageBytes);

            while (true)
            {
                long result = publication.Offer(buffer, 0, buffer.Capacity);
                if (result >= 0)
                {
                    break;
                }
                else if (result == Publication.BACK_PRESSURED)
                {
                    Console.WriteLine("Offer failed due to back pressure. Retrying...");
                    await Task.Yield();
                }
                else if (result == Publication.NOT_CONNECTED)
                {
                    Console.WriteLine("Offer failed because the publication is not connected.");
                    await Task.Yield();
                }
                else if (result == Publication.ADMIN_ACTION)
                {
                    Console.WriteLine("Offer failed due to an admin action in the system.");
                    await Task.Yield();
                }
                else
                {
                    Console.WriteLine($"Offer failed due to unknown reason: {result}");
                    await Task.Yield();
                }
            }
        }
    }
}