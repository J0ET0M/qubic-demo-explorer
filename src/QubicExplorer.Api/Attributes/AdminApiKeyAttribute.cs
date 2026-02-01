using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;

namespace QubicExplorer.Api.Attributes;

/// <summary>
/// Attribute that requires a valid admin API key in the X-Api-Key header.
/// If no API key is configured in settings, all requests are denied.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Method)]
public class AdminApiKeyAttribute : Attribute, IAsyncActionFilter
{
    private const string ApiKeyHeaderName = "X-Api-Key";

    public async Task OnActionExecutionAsync(ActionExecutingContext context, ActionExecutionDelegate next)
    {
        var configuration = context.HttpContext.RequestServices.GetRequiredService<IConfiguration>();
        var configuredApiKey = configuration["AdminApiKey"];

        // If no API key is configured, deny all requests
        if (string.IsNullOrWhiteSpace(configuredApiKey))
        {
            context.Result = new JsonResult(new { error = "Admin API is not configured" })
            {
                StatusCode = StatusCodes.Status503ServiceUnavailable
            };
            return;
        }

        // Check for API key in header
        if (!context.HttpContext.Request.Headers.TryGetValue(ApiKeyHeaderName, out var providedApiKey))
        {
            context.Result = new JsonResult(new { error = "API key is required" })
            {
                StatusCode = StatusCodes.Status401Unauthorized
            };
            return;
        }

        // Validate API key
        if (!string.Equals(configuredApiKey, providedApiKey, StringComparison.Ordinal))
        {
            context.Result = new JsonResult(new { error = "Invalid API key" })
            {
                StatusCode = StatusCodes.Status401Unauthorized
            };
            return;
        }

        await next();
    }
}
