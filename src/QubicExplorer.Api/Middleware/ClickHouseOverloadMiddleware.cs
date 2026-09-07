using System.Text.Json;
using ClickHouse.Client;

namespace QubicExplorer.Api.Middleware;

/// <summary>
/// Catches ClickHouse thread-pool exhaustion errors (Code 439,
/// CANNOT_SCHEDULE_TASK) and translates them into a clean HTTP 503
/// with a Retry-After header, instead of an unhandled 500 with a
/// screenful of stack trace.
///
/// Why bother:
///   1. Without this, every concurrent request that hits an overloaded CH
///      logs a full stack trace at Fail level — the log becomes unreadable
///      exactly when you need it most.
///   2. Clients (browsers, SignalR reconnects, the frontend's fetch helpers)
///      treat a 500 as a hard bug and either fail loudly or hammer the
///      endpoint again. A 503 with Retry-After is the standard signal for
///      "the backend is busy — try again shortly" and well-behaved clients
///      back off.
///
/// Everything that isn't a 439 is left to the default handler.
/// </summary>
public class ClickHouseOverloadMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ILogger<ClickHouseOverloadMiddleware> _logger;

    public ClickHouseOverloadMiddleware(RequestDelegate next, ILogger<ClickHouseOverloadMiddleware> logger)
    {
        _next = next;
        _logger = logger;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        try
        {
            await _next(context);
        }
        catch (Exception ex) when (IsClickHouseOverloaded(ex))
        {
            // One concise warning per overloaded request. Info alone would hide a
            // sustained event, but Fail/Error with the stack is noise. Include the
            // request path so we can see which endpoints are hot.
            _logger.LogWarning(
                "ClickHouse overloaded (Code 439) on {Method} {Path} — returning 503",
                context.Request.Method, context.Request.Path);

            if (!context.Response.HasStarted)
            {
                context.Response.Clear();
                context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
                // Ask well-behaved clients (browsers, SignalR retry, our own
                // fetch composables) to wait 5 s before retrying. Tune later if
                // CH tuning knobs change; 5 s is well within the ~15 s browser
                // fetch timeout so requests will retry within the same page load.
                context.Response.Headers["Retry-After"] = "5";
                context.Response.ContentType = "application/problem+json";
                var payload = JsonSerializer.Serialize(new
                {
                    type = "https://qubic.li/errors/clickhouse-overloaded",
                    title = "ClickHouse is overloaded",
                    status = 503,
                    detail = "The backend database is temporarily unable to schedule new queries. Retry in a few seconds.",
                });
                await context.Response.WriteAsync(payload);
            }
        }
    }

    /// <summary>
    /// Recursively walks the exception chain looking for CH Code 439 or a
    /// CANNOT_SCHEDULE_TASK message. Same detection logic already used by
    /// the indexer/writer backoff paths — kept in sync here.
    /// </summary>
    private static bool IsClickHouseOverloaded(Exception ex)
    {
        for (var e = ex; e != null; e = e.InnerException)
        {
            if (e is ClickHouseServerException chEx && chEx.ErrorCode == 439) return true;
            if (e.Message.Contains("CANNOT_SCHEDULE_TASK", StringComparison.Ordinal)) return true;
        }
        return false;
    }
}
