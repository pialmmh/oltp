# PhotonCache Java Bundle Exporter + Gist Updater (with forced replace)

$packagePath = "src/main/java/com/telcobright/rtc/photoncache"
$outputFile = "all-classes.txt"
$gistId = "0d878d8d2c128a5be5e280288f49cbb2"

# Remove previous output file if exists
if (Test-Path $outputFile) {
    Remove-Item $outputFile -Force
}

# Export all Java classes to all-classes.txt
Get-ChildItem -Path $packagePath -Recurse -Filter *.java | ForEach-Object {
    Add-Content -Path $outputFile -Value "`n// ===== $($_.Name) ====="
    Get-Content $_.FullName | Add-Content -Path $outputFile
}

Write-Host "✅ Exported all Java classes to $outputFile"

# === Replace file in Gist ===
try {
    # Delete the existing file from Gist
    & gh gist edit $gistId --remove all-classes.txt

    # Add the new version
    & gh gist edit $gistId --add $outputFile

    Write-Host "🚀 Gist forcibly updated at: https://gist.github.com/$gistId"
} catch {
    Write-Host "❌ Gist update failed. Check your gh CLI setup." -ForegroundColor Red
}

# Optional: Open the file locally
Start-Process -FilePath $outputFile
