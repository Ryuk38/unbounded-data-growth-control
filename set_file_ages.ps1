# =========================
# Set custom ages for test_data files
# This forces all files into their 'warm' tier action for the experiment
# =========================

Write-Host "⏳ Updating file modification times..."

$age = (Get-Date).AddDays(-2) # 2 days old

# Set all files to be 2 days old
(Get-Item test_data\app.log).LastWriteTime = $age
(Get-Item test_data\app2.log).LastWriteTime = $age
(Get-Item test_data\redundant1.bin).LastWriteTime = $age
(Get-Item test_data\redundant2.bin).LastWriteTime = $age
(Get-Item test_data\redundant3.bin).LastWriteTime = $age
(Get-Item test_data\sensor.csv).LastWriteTime = $age

# Leave the baseline file as 'new' so it stays in the hot tier
(Get-Item test_data\raw_baseline.dat).LastWriteTime = (Get-Date)

Write-Host "✅ File timestamps updated. Now run: python main.py"