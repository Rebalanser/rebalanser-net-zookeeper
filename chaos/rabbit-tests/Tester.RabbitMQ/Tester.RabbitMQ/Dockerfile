FROM microsoft/dotnet:2.1-sdk AS build
WORKDIR /app

COPY *.csproj ./
RUN dotnet restore

COPY . ./
RUN dotnet publish -c Release -o output

FROM microsoft/dotnet:2.1-runtime AS runtime
COPY --from=build /app/output .
ENTRYPOINT ["dotnet", "Tester.RabbitMQ.dll"]