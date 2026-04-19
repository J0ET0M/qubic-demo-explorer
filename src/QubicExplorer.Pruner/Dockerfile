FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Copy project files
COPY QubicExplorer.Shared/*.csproj ./QubicExplorer.Shared/
COPY QubicExplorer.Pruner/*.csproj ./QubicExplorer.Pruner/

# Restore dependencies
RUN dotnet restore QubicExplorer.Pruner/QubicExplorer.Pruner.csproj

# Copy source code
COPY QubicExplorer.Shared/ ./QubicExplorer.Shared/
COPY QubicExplorer.Pruner/ ./QubicExplorer.Pruner/

# Build
WORKDIR /src/QubicExplorer.Pruner
RUN dotnet publish -c Release -o /app/publish

# Runtime image
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "QubicExplorer.Pruner.dll"]
