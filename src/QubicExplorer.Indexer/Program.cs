using QubicExplorer.Indexer.Services;
using QubicExplorer.Shared.Configuration;
using IndexerOptions = QubicExplorer.Indexer.Configuration.IndexerOptions;

var builder = Host.CreateApplicationBuilder(args);

// Configure console logging with timestamps
builder.Logging.AddSimpleConsole(options =>
{
    options.TimestampFormat = "yyyy-MM-dd HH:mm:ss ";
    options.IncludeScopes = false;
});

// Configure Seq logging (optional - only if Seq:ServerUrl is configured)
var seqUrl = builder.Configuration["Seq:ServerUrl"];
if (!string.IsNullOrEmpty(seqUrl))
{
    builder.Logging.AddSeq(seqUrl, builder.Configuration["Seq:ApiKey"]);
}

// Configure options
builder.Services.Configure<BobOptions>(builder.Configuration.GetSection(BobOptions.SectionName));
builder.Services.Configure<ClickHouseOptions>(builder.Configuration.GetSection(ClickHouseOptions.SectionName));
builder.Services.Configure<IndexerOptions>(builder.Configuration.GetSection(IndexerOptions.SectionName));

// Register services
builder.Services.AddSingleton<BobConnectionService>();
builder.Services.AddSingleton<ClickHouseWriterService>();
builder.Services.AddHostedService<IndexerWorker>();

var host = builder.Build();
host.Run();
