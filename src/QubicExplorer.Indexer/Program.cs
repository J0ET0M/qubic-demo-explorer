using QubicExplorer.Indexer.Configuration;
using QubicExplorer.Indexer.Services;

var builder = Host.CreateApplicationBuilder(args);

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
