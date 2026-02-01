FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Copy project files
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
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "QubicExplorer.Indexer.dll"]
