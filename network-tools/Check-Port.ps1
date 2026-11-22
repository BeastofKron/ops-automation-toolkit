param(
    [string]$targetHost = "localhost",
    [int]$port = 1234
)

try {
    $connection = Test-NetConnection -ComputerName $targetHost -Port $port
    if ($connection.TcpTestSucceeded) {
        Write-Host "✅ Port $port is open on $targetHost"
    } else {
        Write-Host "❌ Port $port is CLOSED on $targetHost"
    }
} catch {
    Write-Host "⚠️ Error testing ${targetHost}:${port}"
}
