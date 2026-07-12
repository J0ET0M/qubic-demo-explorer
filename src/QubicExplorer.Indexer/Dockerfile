FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# NuGet source selection.
#   Default:   nuget.org only        (works for anyone cloning the repo)
#   Opt-in:    nuget.org + local/    (docker build --build-arg USE_LOCAL_PACKAGES=true)
ARG USE_LOCAL_PACKAGES=false

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
COPY QubicExplorer.Indexer/*.csproj ./QubicExplorer.Indexer/

# Restore dependencies
RUN dotnet restore QubicExplorer.Indexer/QubicExplorer.Indexer.csproj

# Copy source code
COPY QubicExplorer.Shared/ ./QubicExplorer.Shared/
COPY QubicExplorer.Indexer/ ./QubicExplorer.Indexer/

# Build
WORKDIR /src/QubicExplorer.Indexer
RUN dotnet publish -c Release -o /app/publish

# Runtime image
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "QubicExplorer.Indexer.dll"]
