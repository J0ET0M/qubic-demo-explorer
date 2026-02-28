FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Copy project files
COPY QubicExplorer.Shared/*.csproj ./QubicExplorer.Shared/
COPY QubicExplorer.Analytics/*.csproj ./QubicExplorer.Analytics/

# Restore dependencies
RUN dotnet restore QubicExplorer.Analytics/QubicExplorer.Analytics.csproj

# Copy source code
COPY QubicExplorer.Shared/ ./QubicExplorer.Shared/
COPY QubicExplorer.Analytics/ ./QubicExplorer.Analytics/

# Build
WORKDIR /src/QubicExplorer.Analytics
RUN dotnet publish -c Release -o /app/publish

# Runtime image
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "QubicExplorer.Analytics.dll"]
