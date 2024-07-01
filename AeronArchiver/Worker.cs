using System;
using System.Threading;
using System.Threading.Tasks;
using Adaptive.Aeron;
using Adaptive.Archiver;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AeronArchiver;

public class Worker(ILogger<Worker> logger) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        Aeron.Context context = new();

        using Aeron aeron = Aeron.Connect(context);

        Console.WriteLine("Connected to Aeron");

        AeronArchive.Context archiveContext = new AeronArchive.Context().AeronClient(aeron).ControlRequestChannel("aeron:udp?endpoint=localhost:8010");

        using AeronArchive archive = AeronArchive.Connect(archiveContext);

        Console.WriteLine("Connected to Aeron Archive");

        //archive.StartRecording()

        return Task.CompletedTask;
    }
}