using ClickHouse.Client.ADO;
using Microsoft.Extensions.Options;
using Qubic.Bob;
using QubicExplorer.Analytics.Services;
using QubicExplorer.Shared;
using QubicExplorer.Shared.Configuration;
using QubicExplorer.Shared.Services;

var builder = WebApplication.CreateBuilder(args);

// Configure console logging with timestamps
builder.Logging.AddSimpleConsole(options =>
{
    options.TimestampFormat = "yyyy-MM-dd HH:mm:ss ";
    options.IncludeScopes = false;
});

// Configure Seq logging
var seqUrl = builder.Configuration["Seq:ServerUrl"];
if (!string.IsNullOrEmpty(seqUrl))
{
    builder.Logging.AddSeq(seqUrl, builder.Configuration["Seq:ApiKey"]);
}

// Configure options
builder.Services.Configure<ClickHouseOptions>(builder.Configuration.GetSection(ClickHouseOptions.SectionName));
builder.Services.Configure<BobOptions>(builder.Configuration.GetSection(BobOptions.SectionName));
builder.Services.Configure<AddressLabelOptions>(builder.Configuration.GetSection(AddressLabelOptions.SectionName));

// Add memory cache for BobProxyService
builder.Services.AddMemoryCache();

// Add HttpClient for AddressLabelService
builder.Services.AddHttpClient();

// Register services
builder.Services.AddSingleton<AnalyticsCacheService>();
builder.Services.AddSingleton<AnalyticsQueryService>();

// AddressLabelService - fetches and caches address labels
builder.Services.AddSingleton<AddressLabelService>(sp =>
{
    var httpClientFactory = sp.GetRequiredService<IHttpClientFactory>();
    var options = sp.GetRequiredService<IOptions<AddressLabelOptions>>().Value;
    var logger = sp.GetRequiredService<ILogger<AddressLabelService>>();
    return new AddressLabelService(httpClientFactory.CreateClient(), options.BundleUrl, logger);
});

// BobWebSocketClient - shared singleton for Bob communication
builder.Services.AddSingleton<BobWebSocketClient>(sp =>
{
    var bobOptions = sp.GetRequiredService<IOptions<BobOptions>>().Value;
    var wsOptions = new BobWebSocketOptions
    {
        Nodes = bobOptions.Nodes.ToArray()
    };
    return new BobWebSocketClient(wsOptions);
});

// BobProxyService - cached RPC queries
builder.Services.AddSingleton<BobProxyService>();

// ComputorFlowService - tracks miner/computor money flow
builder.Services.AddSingleton<ComputorFlowService>();

// CustomFlowTrackingService - user-initiated flow tracking
builder.Services.AddSingleton<CustomFlowTrackingService>();

// Background services
builder.Services.AddHostedService<AnalyticsSnapshotService>();

// Add controllers (for admin endpoints)
builder.Services.AddControllers();

var app = builder.Build();

// Ensure ClickHouse database and schema exist
{
    var chOptions = app.Services.GetRequiredService<IOptions<ClickHouseOptions>>().Value;
    var logger = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("SchemaInit");
    using var serverConn = new ClickHouseConnection(chOptions.ServerConnectionString);
    await serverConn.OpenAsync();

    await using (var cmd = serverConn.CreateCommand())
    {
        cmd.CommandText = ClickHouseSchema.CreateDatabase;
        await cmd.ExecuteNonQueryAsync();
    }

    logger.LogInformation("Ensured database '{Database}' exists", chOptions.Database);

    var statements = ClickHouseSchema.GetSchemaStatements();
    foreach (var sql in statements)
    {
        await using var cmd = serverConn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    logger.LogInformation("Schema initialization complete ({Count} statements)", statements.Count);
}

// Connect BobWebSocketClient at startup
var bobClient = app.Services.GetRequiredService<BobWebSocketClient>();
await bobClient.ConnectAsync();

// Initialize AddressLabelService at startup
var addressLabelService = app.Services.GetRequiredService<AddressLabelService>();
await addressLabelService.InitializeAsync();

// Map admin endpoints
app.MapControllers();

// Health check endpoint
app.MapGet("/health", () => Results.Ok(new { status = "healthy", service = "analytics" }));

app.Run();
