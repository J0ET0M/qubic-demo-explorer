using QubicExplorer.Pruner.Configuration;
using QubicExplorer.Pruner.Services;
using QubicExplorer.Shared.Configuration;

var builder = Host.CreateApplicationBuilder(args);

// Configuration
builder.Services.Configure<ClickHouseOptions>(builder.Configuration.GetSection(ClickHouseOptions.SectionName));
builder.Services.Configure<PrunerOptions>(builder.Configuration.GetSection(PrunerOptions.SectionName));

// Seq logging
var seqUrl = builder.Configuration["Seq:ServerUrl"];
if (!string.IsNullOrEmpty(seqUrl))
{
    builder.Logging.AddSeq(seqUrl, apiKey: builder.Configuration["Seq:ApiKey"]);
}

// Background service
builder.Services.AddHostedService<PrunerService>();

var host = builder.Build();
host.Run();
