$ErrorActionPreference = "Stop"
$es = "http://localhost:9200"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

function Bulk([string]$path) {
  Write-Host "Bulk -> $path" -ForegroundColor Cyan
  $body = Get-Content -Raw -Path $path -Encoding UTF8
  $resp = Invoke-RestMethod -Method Post -Uri "$es/_bulk" -ContentType 'application/x-ndjson; charset=utf-8' -Body $body
  if ($null -eq $resp) {
    Write-Warning "Empty response from _bulk"
    return
  }
  $items = @($resp.items).Count
  $failed = (@($resp.items) | Where-Object { $_.index.status -ge 300 -or $_.create.status -ge 300 -or $_.update.status -ge 300 -or $_.delete.status -ge 300 }).Count
  Write-Host "Bulk result: errors=$($resp.errors) items=$items failed=$failed" -ForegroundColor Yellow
  if ($resp.errors -eq $true -or $failed -gt 0) {
    $errs = @($resp.items) | ForEach-Object { $_.index, $_.create, $_.update, $_.delete } | Where-Object { $_ -and $_.status -ge 300 } | Select-Object -First 5
    Write-Warning ("Sample errors: " + ($errs | ConvertTo-Json -Depth 6))
  }
}

try {
  $info = Invoke-RestMethod -Method Get -Uri $es
  Write-Host "Connected to Elasticsearch: $($info.name) ($($info.version.number))" -ForegroundColor Green
} catch {
  Write-Error "Failed to connect to $es. Make sure docker compose is running (docker compose up -d)."
  exit 1
}

Bulk "$(Resolve-Path ./data/books_sample.ndjson)"
Bulk "$(Resolve-Path ./data/book_content_sample.ndjson)"

Write-Host "Done: sample data loaded." -ForegroundColor Green

