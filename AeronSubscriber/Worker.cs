using System;
using System.Threading;
using System.Threading.Tasks;
using Adaptive.Aeron;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AeronSubscriber;

public class Worker(ILogger<Worker> logger) : BackgroundService
{
    private const string Channel = "aeron:ipc";
    private const int StreamId = 10;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var ctx = new Aeron.Context().AeronDirectoryName("/aeron-ipc");

        using (var aeron = Aeron.Connect(ctx))
        using (var subscription = aeron.AddSubscription(Channel, StreamId))
        {
            var fragmentHandler = new FragmentAssembler((buffer, offset, length, header) =>
            {
                var message = buffer.GetStringWithoutLengthUtf8(offset, length);
                Console.WriteLine("Received: " + message);
            });

            while (true)
            {
                int fragments = subscription.Poll(fragmentHandler, 10);
                if (fragments == 0)
                {
                    Thread.Yield();
                }
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
}