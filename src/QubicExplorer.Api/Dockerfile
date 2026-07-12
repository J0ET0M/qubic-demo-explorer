FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# NuGet source selection.
#   Default:   nuget.org only        (works for anyone cloning the repo)
#   Opt-in:    nuget.org + local/    (docker build --build-arg USE_LOCAL_PACKAGES=true)
# The maintainer-only feed is used when you're testing pre-release Qubic.*
# packages that aren't published yet — drop them into ./local-packages/ first.
ARG USE_LOCAL_PACKAGES=false

# Copy both configs and (optionally-empty) local-packages folder.
COPY NuGet.config NuGet.local.config ./
COPY local-packages/ ./local-packages/
RUN if [ "$USE_LOCAL_PACKAGES" = "true" ]; then \
        mv NuGet.local.config NuGet.config && \
        echo "→ Building with LOCAL package overrides from ./local-packages/"; \
    else \
        rm -f NuGet.local.config && \
        echo "→ Building from public nuget.org only"; \
    fi

COPY QubicExplorer.Shared/*.csproj ./QubicExplorer.Shared/
COPY QubicExplorer.Api/*.csproj ./QubicExplorer.Api/

# Restore dependencies
RUN dotnet restore QubicExplorer.Api/QubicExplorer.Api.csproj

# Copy source code
COPY QubicExplorer.Shared/ ./QubicExplorer.Shared/
COPY QubicExplorer.Api/ ./QubicExplorer.Api/

# Build
WORKDIR /src/QubicExplorer.Api
RUN dotnet publish -c Release -o /app/publish

# Runtime image
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .

EXPOSE 8080
ENTRYPOINT ["dotnet", "QubicExplorer.Api.dll"]
