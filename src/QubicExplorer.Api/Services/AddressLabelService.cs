using System.Text.Json;
using QubicExplorer.Shared.Models;

namespace QubicExplorer.Api.Services;

public class AddressLabelService
{
    private readonly ILogger<AddressLabelService> _logger;
    private readonly HttpClient _httpClient;
    private readonly string _bundleUrl;
    private readonly Dictionary<string, string> _addressLabels = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, AddressInfo> _addressInfo = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, Dictionary<int, string>> _contractProcedures = new(StringComparer.OrdinalIgnoreCase);
    private readonly object _lock = new();
    private DateTime _lastUpdate = DateTime.MinValue;
    private readonly TimeSpan _cacheExpiry = TimeSpan.FromHours(1);

    // the burn address is also the zero address
    // zero address can be the source of emissions
    public const string BurnAddress =      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB";
    
    private static readonly HashSet<string> BurnAddresses = new(StringComparer.OrdinalIgnoreCase)
    {
        BurnAddress
    };

    public AddressLabelService(HttpClient httpClient, string bundleUrl, ILogger<AddressLabelService> logger)
    {
        _httpClient = httpClient;
        _bundleUrl = bundleUrl;
        _logger = logger;
    }

    public async Task InitializeAsync() => await RefreshLabelsAsync();

    public async Task RefreshLabelsAsync()
    {
        try
        {
            _logger.LogInformation("Fetching address labels from {BundleUrl}...", _bundleUrl);
            var response = await _httpClient.GetStringAsync(_bundleUrl);
            var bundle = JsonSerializer.Deserialize<BundleData>(response);

            if (bundle == null)
            {
                _logger.LogWarning("Failed to deserialize bundle data");
                return;
            }

            lock (_lock)
            {
                _addressLabels.Clear();
                _addressInfo.Clear();
                _contractProcedures.Clear();

                foreach (var label in bundle.AddressLabels)
                {
                    if (!string.IsNullOrEmpty(label.Address))
                    {
                        _addressLabels[label.Address] = label.Label ?? label.Name;
                        _addressInfo[label.Address] = new AddressInfo
                        {
                            Label = label.Label ?? label.Name,
                            Type = AddressType.Known
                        };
                    }
                }

                foreach (var exchange in bundle.Exchanges)
                {
                    if (!string.IsNullOrEmpty(exchange.Address))
                    {
                        _addressLabels[exchange.Address] = $"#{exchange.Name}";
                        _addressInfo[exchange.Address] = new AddressInfo
                        {
                            Label = exchange.Name,
                            Type = AddressType.Exchange
                        };
                    }
                }

                foreach (var contract in bundle.SmartContracts)
                {
                    if (!string.IsNullOrEmpty(contract.Address))
                    {
                        _addressLabels[contract.Address] = $"[{contract.Name}]";
                        _addressInfo[contract.Address] = new AddressInfo
                        {
                            Label = contract.Name,
                            Type = AddressType.SmartContract,
                            ContractIndex = contract.ContractIndex,
                            Website = contract.Website
                        };

                        // Store procedures for this contract
                        if (contract.Procedures != null && contract.Procedures.Count > 0)
                        {
                            var procedures = new Dictionary<int, string>();
                            foreach (var proc in contract.Procedures)
                            {
                                procedures[proc.Id] = proc.Name;
                            }
                            _contractProcedures[contract.Address] = procedures;
                        }
                    }
                }

                foreach (var token in bundle.Tokens)
                {
                    if (!string.IsNullOrEmpty(token.Issuer) && !_addressLabels.ContainsKey(token.Issuer))
                    {
                        _addressLabels[token.Issuer] = $"${token.Name} Issuer";
                        _addressInfo[token.Issuer] = new AddressInfo
                        {
                            Label = $"{token.Name} Issuer",
                            Type = AddressType.TokenIssuer,
                            Website = token.Website
                        };
                    }
                }

                _addressLabels[BurnAddress] = "BURN";
                _addressInfo[BurnAddress] = new AddressInfo { Label = "BURN", Type = AddressType.Burn };

                _lastUpdate = DateTime.UtcNow;
            }

            _logger.LogInformation("Loaded {Count} address labels", _addressLabels.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error fetching address labels from {BundleUrl}", _bundleUrl);
        }
    }

    public string? GetLabel(string address)
    {
        if (string.IsNullOrEmpty(address))
            return null;

        lock (_lock)
        {
            if (_addressLabels.TryGetValue(address, out var label))
                return label;
        }

        return null;
    }

    public AddressInfo? GetAddressInfo(string address)
    {
        if (string.IsNullOrEmpty(address))
            return null;

        lock (_lock)
        {
            if (_addressInfo.TryGetValue(address, out var info))
                return info;
        }

        return null;
    }

    public Dictionary<string, AddressInfo> GetLabelsForAddresses(IEnumerable<string> addresses)
    {
        var result = new Dictionary<string, AddressInfo>();

        lock (_lock)
        {
            foreach (var address in addresses)
            {
                if (!string.IsNullOrEmpty(address) && _addressInfo.TryGetValue(address, out var info))
                {
                    result[address] = info;
                }
            }
        }

        return result;
    }

    public bool IsBurnAddress(string address) =>
        !string.IsNullOrEmpty(address) && BurnAddresses.Contains(address);

    public bool IsExchange(string address)
    {
        lock (_lock)
        {
            if (_addressInfo.TryGetValue(address, out var info))
                return info.Type == AddressType.Exchange;
        }
        return false;
    }

    public bool IsSmartContract(string address)
    {
        lock (_lock)
        {
            if (_addressInfo.TryGetValue(address, out var info))
                return info.Type == AddressType.SmartContract;
        }
        return false;
    }

    public string? GetProcedureName(string contractAddress, int inputType)
    {
        if (string.IsNullOrEmpty(contractAddress) || inputType <= 0)
            return null;

        lock (_lock)
        {
            if (_contractProcedures.TryGetValue(contractAddress, out var procedures))
            {
                if (procedures.TryGetValue(inputType, out var name))
                    return name;
            }
        }
        return null;
    }

    public async Task EnsureFreshDataAsync()
    {
        if (DateTime.UtcNow - _lastUpdate > _cacheExpiry)
            await RefreshLabelsAsync();
    }

    public int LabelCount
    {
        get
        {
            lock (_lock)
            {
                return _addressLabels.Count;
            }
        }
    }

    public List<AddressInfoDto> GetAllAddresses(string? type = null)
    {
        lock (_lock)
        {
            var query = _addressInfo.AsEnumerable();

            if (!string.IsNullOrEmpty(type) && Enum.TryParse<AddressType>(type, true, out var addressType))
            {
                query = query.Where(kvp => kvp.Value.Type == addressType);
            }

            return query
                .Select(kvp => new AddressInfoDto
                {
                    Address = kvp.Key,
                    Label = kvp.Value.Label,
                    Type = kvp.Value.Type.ToString().ToLowerInvariant(),
                    ContractIndex = kvp.Value.ContractIndex,
                    Website = kvp.Value.Website
                })
                .OrderBy(a => a.Type)
                .ThenBy(a => a.Label)
                .ToList();
        }
    }

    public Dictionary<string, int> GetTypeCounts()
    {
        lock (_lock)
        {
            return _addressInfo
                .GroupBy(kvp => kvp.Value.Type.ToString().ToLowerInvariant())
                .ToDictionary(g => g.Key, g => g.Count());
        }
    }

    /// <summary>
    /// Get all addresses of a specific type
    /// </summary>
    public List<AddressInfoDto> GetAddressesByType(AddressType type)
    {
        lock (_lock)
        {
            return _addressInfo
                .Where(kvp => kvp.Value.Type == type)
                .Select(kvp => new AddressInfoDto
                {
                    Address = kvp.Key,
                    Label = kvp.Value.Label,
                    Type = kvp.Value.Type.ToString().ToLowerInvariant(),
                    ContractIndex = kvp.Value.ContractIndex,
                    Website = kvp.Value.Website
                })
                .ToList();
        }
    }

    /// <summary>
    /// Search for addresses by their label/name (case-insensitive partial match)
    /// </summary>
    public List<AddressInfoDto> SearchByLabel(string query, int maxResults = 10)
    {
        if (string.IsNullOrWhiteSpace(query))
            return new List<AddressInfoDto>();

        var searchTerms = query.ToLowerInvariant().Split(' ', StringSplitOptions.RemoveEmptyEntries);

        lock (_lock)
        {
            return _addressInfo
                .Where(kvp =>
                {
                    var label = kvp.Value.Label.ToLowerInvariant();
                    // All search terms must be present in the label
                    return searchTerms.All(term => label.Contains(term));
                })
                .OrderByDescending(kvp =>
                {
                    // Prioritize exact matches, then prefix matches, then contains
                    var label = kvp.Value.Label.ToLowerInvariant();
                    var queryLower = query.ToLowerInvariant();
                    if (label == queryLower) return 100;
                    if (label.StartsWith(queryLower)) return 50;
                    return 0;
                })
                .ThenBy(kvp => kvp.Value.Label.Length) // Shorter labels first
                .Take(maxResults)
                .Select(kvp => new AddressInfoDto
                {
                    Address = kvp.Key,
                    Label = kvp.Value.Label,
                    Type = kvp.Value.Type.ToString().ToLowerInvariant(),
                    ContractIndex = kvp.Value.ContractIndex,
                    Website = kvp.Value.Website
                })
                .ToList();
        }
    }
}

public class AddressInfoDto
{
    public string Address { get; set; } = "";
    public string Label { get; set; } = "";
    public string Type { get; set; } = "";
    public int? ContractIndex { get; set; }
    public string? Website { get; set; }
}

public class AddressInfo
{
    public string Label { get; set; } = "";
    public AddressType Type { get; set; }
    public int? ContractIndex { get; set; }
    public string? Website { get; set; }
}

public enum AddressType
{
    Unknown,
    Known,
    Exchange,
    SmartContract,
    TokenIssuer,
    Burn
}
