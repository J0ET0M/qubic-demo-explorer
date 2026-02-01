using System.Text.Json;
using System.Text.Json.Serialization;

namespace QubicExplorer.Indexer.Models;

// Custom converter to handle amounts that may come as strings or numbers
public class StringOrNumberToUInt64Converter : JsonConverter<ulong>
{
    public override ulong Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.String)
        {
            var stringValue = reader.GetString();
            if (ulong.TryParse(stringValue, out var result))
                return result;
            return 0;
        }
        if (reader.TokenType == JsonTokenType.Number)
        {
            return reader.GetUInt64();
        }
        return 0;
    }

    public override void Write(Utf8JsonWriter writer, ulong value, JsonSerializerOptions options)
    {
        writer.WriteNumberValue(value);
    }
}

// TickStream data structures — used by ClickHouseWriterService and IndexerWorker
public class TickStreamData
{
    [JsonPropertyName("epoch")]
    public uint Epoch { get; set; }

    [JsonPropertyName("tick")]
    public ulong Tick { get; set; }

    [JsonPropertyName("isCatchUp")]
    public bool IsCatchUp { get; set; }

    [JsonPropertyName("timestamp")]
    public string? Timestamp { get; set; }

    [JsonPropertyName("txCountFiltered")]
    public uint TxCountFiltered { get; set; }

    [JsonPropertyName("txCountTotal")]
    public uint TxCountTotal { get; set; }

    [JsonPropertyName("logCountFiltered")]
    public uint LogCountFiltered { get; set; }

    [JsonPropertyName("logCountTotal")]
    public uint LogCountTotal { get; set; }

    [JsonPropertyName("transactions")]
    public List<BobTransaction>? Transactions { get; set; }

    [JsonPropertyName("logs")]
    public List<BobLog>? Logs { get; set; }
}

public class BobTransaction
{
    [JsonPropertyName("hash")]
    public string Hash { get; set; } = string.Empty;

    [JsonPropertyName("from")]
    public string From { get; set; } = string.Empty;

    [JsonPropertyName("to")]
    public string To { get; set; } = string.Empty;

    [JsonPropertyName("amount")]
    [JsonConverter(typeof(StringOrNumberToUInt64Converter))]
    public ulong Amount { get; set; }

    [JsonPropertyName("inputType")]
    public ushort InputType { get; set; }

    [JsonPropertyName("inputData")]
    public string? InputData { get; set; }

    [JsonPropertyName("executed")]
    public bool Executed { get; set; }

    [JsonPropertyName("logIdFrom")]
    public int LogIdFrom { get; set; }

    [JsonPropertyName("logIdLength")]
    public ushort LogIdLength { get; set; }
}

// Log type constants matching Bob's LogEvent types
public static class BobLogTypes
{
    public const byte QuTransfer = 0;
    public const byte AssetIssuance = 1;
    public const byte AssetOwnershipChange = 2;
    public const byte AssetPossessionChange = 3;
    public const byte ContractErrorMessage = 4;
    public const byte ContractWarningMessage = 5;
    public const byte ContractInformationMessage = 6;
    public const byte ContractDebugMessage = 7;
    public const byte Burning = 8;
    public const byte DustBurning = 9;
    public const byte SpectrumStats = 10;
    public const byte AssetOwnershipManagingContractChange = 11;
    public const byte AssetPossessionManagingContractChange = 12;
    public const byte ContractReserveDeduction = 13;
    public const byte CustomMessage = 255;
}

public class BobLog
{
    [JsonPropertyName("ok")]
    public bool Ok { get; set; }

    [JsonPropertyName("tick")]
    public ulong Tick { get; set; }

    [JsonPropertyName("epoch")]
    public uint Epoch { get; set; }

    [JsonPropertyName("logId")]
    public uint LogId { get; set; }

    [JsonPropertyName("type")]
    public byte LogType { get; set; }

    [JsonPropertyName("logTypename")]
    public string LogTypeName { get; set; } = string.Empty;

    // Get log type name with fallback for known types
    public string GetLogTypeName()
    {
        if (!string.IsNullOrEmpty(LogTypeName))
            return LogTypeName;

        return LogType switch
        {
            BobLogTypes.QuTransfer => "QU_TRANSFER",
            BobLogTypes.AssetIssuance => "ASSET_ISSUANCE",
            BobLogTypes.AssetOwnershipChange => "ASSET_OWNERSHIP_CHANGE",
            BobLogTypes.AssetPossessionChange => "ASSET_POSSESSION_CHANGE",
            BobLogTypes.ContractErrorMessage => "CONTRACT_ERROR_MESSAGE",
            BobLogTypes.ContractWarningMessage => "CONTRACT_WARNING_MESSAGE",
            BobLogTypes.ContractInformationMessage => "CONTRACT_INFORMATION_MESSAGE",
            BobLogTypes.ContractDebugMessage => "CONTRACT_DEBUG_MESSAGE",
            BobLogTypes.Burning => "BURNING",
            BobLogTypes.DustBurning => "DUST_BURNING",
            BobLogTypes.SpectrumStats => "SPECTRUM_STATS",
            BobLogTypes.AssetOwnershipManagingContractChange => "ASSET_OWNERSHIP_MANAGING_CONTRACT_CHANGE",
            BobLogTypes.AssetPossessionManagingContractChange => "ASSET_POSSESSION_MANAGING_CONTRACT_CHANGE",
            BobLogTypes.ContractReserveDeduction => "CONTRACT_RESERVE_DEDUCTION",
            BobLogTypes.CustomMessage => "CUSTOM_MESSAGE",
            _ => $"UNKNOWN_{LogType}"
        };
    }

    [JsonPropertyName("logDigest")]
    public string? LogDigest { get; set; }

    [JsonPropertyName("bodySize")]
    public int BodySize { get; set; }

    [JsonPropertyName("timestamp")]
    public string? Timestamp { get; set; }

    [JsonPropertyName("txHash")]
    public string? TxHash { get; set; }

    // Dynamic body - structure depends on LogType
    [JsonPropertyName("body")]
    public JsonElement? Body { get; set; }

    // Legacy fields for backwards compatibility
    [JsonPropertyName("source")]
    public string? Source { get; set; }

    [JsonPropertyName("dest")]
    public string? Dest { get; set; }

    [JsonPropertyName("amount")]
    [JsonConverter(typeof(StringOrNumberToUInt64Converter))]
    public ulong Amount { get; set; }

    [JsonPropertyName("assetName")]
    public string? AssetName { get; set; }

    [JsonPropertyName("rawData")]
    public string? RawData { get; set; }

    // Helper to get source address based on log type
    public string? GetSourceAddress()
    {
        if (Body.HasValue && Body.Value.ValueKind == JsonValueKind.Object)
        {
            return LogType switch
            {
                BobLogTypes.QuTransfer => GetBodyString("from"),
                BobLogTypes.AssetIssuance => GetBodyString("issuerPublicKey"),
                BobLogTypes.AssetOwnershipChange => GetBodyString("sourcePublicKey"),
                BobLogTypes.AssetPossessionChange => GetBodyString("sourcePublicKey"),
                BobLogTypes.Burning => GetBodyString("publicKey"),
                _ => GetBodyString("from") ?? GetBodyString("sourcePublicKey")
            };
        }
        return Source;
    }

    // Helper to get destination address based on log type
    public string? GetDestAddress()
    {
        if (Body.HasValue && Body.Value.ValueKind == JsonValueKind.Object)
        {
            return LogType switch
            {
                BobLogTypes.QuTransfer => GetBodyString("to"),
                BobLogTypes.AssetOwnershipChange => GetBodyString("destinationPublicKey"),
                BobLogTypes.AssetPossessionChange => GetBodyString("destinationPublicKey"),
                _ => GetBodyString("to") ?? GetBodyString("destinationPublicKey") ?? GetBodyString("newOwner")
            };
        }
        return Dest;
    }

    // Helper to get amount based on log type
    public ulong GetAmount()
    {
        if (Body.HasValue && Body.Value.ValueKind == JsonValueKind.Object)
        {
            return LogType switch
            {
                BobLogTypes.QuTransfer => GetBodyUInt64("amount"),
                BobLogTypes.AssetIssuance => GetBodyUInt64("numberOfShares"),
                BobLogTypes.AssetOwnershipChange => GetBodyUInt64("numberOfShares"),
                BobLogTypes.AssetPossessionChange => GetBodyUInt64("numberOfShares"),
                BobLogTypes.Burning => GetBodyUInt64("amount"),
                _ => GetBodyUInt64("amount")
            };
        }
        return Amount;
    }

    // Helper to get asset name based on log type
    public string? GetAssetName()
    {
        if (Body.HasValue && Body.Value.ValueKind == JsonValueKind.Object)
        {
            return LogType switch
            {
                BobLogTypes.AssetIssuance => GetBodyString("name"),
                BobLogTypes.AssetOwnershipChange => GetBodyString("assetName"),
                BobLogTypes.AssetPossessionChange => GetBodyString("assetName"),
                _ => GetBodyString("assetName") ?? GetBodyString("name")
            };
        }
        return AssetName;
    }

    // Get the raw body as JSON string for storage
    public string? GetRawBodyJson()
    {
        if (Body.HasValue && Body.Value.ValueKind != JsonValueKind.Undefined)
        {
            return Body.Value.GetRawText();
        }
        return RawData;
    }

    // Helper methods to extract values from dynamic body
    private string? GetBodyString(string propertyName)
    {
        if (Body.HasValue && Body.Value.TryGetProperty(propertyName, out var prop))
        {
            return prop.ValueKind == JsonValueKind.String ? prop.GetString() : null;
        }
        return null;
    }

    private ulong GetBodyUInt64(string propertyName)
    {
        if (Body.HasValue && Body.Value.TryGetProperty(propertyName, out var prop))
        {
            if (prop.ValueKind == JsonValueKind.Number)
            {
                return prop.GetUInt64();
            }
            if (prop.ValueKind == JsonValueKind.String && ulong.TryParse(prop.GetString(), out var result))
            {
                return result;
            }
        }
        return 0;
    }
}
