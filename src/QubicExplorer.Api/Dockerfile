FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Copy project files
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
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .

EXPOSE 8080
ENTRYPOINT ["dotnet", "QubicExplorer.Api.dll"]
