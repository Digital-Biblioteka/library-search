$ErrorActionPreference = "Stop"
$es = "http://localhost:9200"

function Remove-IfExists([string]$index) {
  try {
    Write-Host "Deleting index $index if exists" -ForegroundColor Yellow
    Invoke-RestMethod -Method Delete -Uri "$es/$index" | Out-Null
  } catch {
  }
}

function Put-Index([string]$index, [string]$mappingPath) {
  Write-Host "Creating index $index from $mappingPath" -ForegroundColor Cyan
  $body = Get-Content -Raw -Path $mappingPath
  Invoke-RestMethod -Method Put -Uri "$es/$index" -ContentType 'application/json' -Body $body | Out-Null
}

try {
  $info = Invoke-RestMethod -Method Get -Uri $es
  Write-Host "Connected to Elasticsearch: $($info.name) ($($info.version.number))" -ForegroundColor Green
} catch {
  Write-Error "Failed to connect to $es. Make sure docker compose is running (docker compose up -d)."
  exit 1
}

Remove-IfExists "books"
Remove-IfExists "book_content"

Put-Index "books" "$(Resolve-Path ./mappings/books.json)"
Put-Index "book_content" "$(Resolve-Path ./mappings/book_content.json)"

Write-Host "Done: indices 'books' and 'book_content' created." -ForegroundColor Green

