param(
    [switch]$Commit,
    [switch]$Push
)

$ErrorActionPreference = "Stop"

function Ensure-Dir($path) {
    if (!(Test-Path $path)) {
        New-Item -ItemType Directory -Path $path | Out-Null
    }
}

function Move-IfExists($file, $dest) {
    if (Test-Path $file) {
        Move-Item $file $dest -Force
    }
}

Write-Host "Creating folders..."

$dirs = @(
    "src",
    "src\converters",
    "src\analysis",
    "src\db",
    "src\utils",
    "data",
    "data\raw",
    "data\parquet_out",
    "config",
    "docs",
    "scripts"
)

foreach ($dir in $dirs) {
    Ensure-Dir $dir
}

Write-Host "Moving files..."

Move-IfExists "csv_to_parquet.py" "src\converters\"
Move-IfExists "csv_split_to_parquet.py" "src\converters\"
Move-IfExists "transform.py" "src\converters\"

Move-IfExists "dive_profile_analysis.py" "src\analysis\"
Move-IfExists "energy_per_run.py" "src\analysis\"
Move-IfExists "humidity_anomaly.py" "src\analysis\"
Move-IfExists "imu_motion_anomaly.py" "src\analysis\"
Move-IfExists "voltage_sag_events.py" "src\analysis\"

Move-IfExists "duckdb_queries.py" "src\db\"
Move-IfExists "duckdb_validate.py" "src\db\"

Move-IfExists "sensors.csv" "data\raw\"
Move-IfExists "sensors_short.csv" "data\raw\"
Move-IfExists "sensors_6S.csv" "data\raw\"

Move-IfExists "data.parquet" "data\parquet_out\"

if (Test-Path "parquet_out") {
    Get-ChildItem "parquet_out" | Move-Item -Destination "data\parquet_out\" -Force
    Remove-Item "parquet_out" -Force
}

Move-IfExists "groups.json" "config\"
Move-IfExists "keyword_groups.json" "config\"

Move-IfExists "schema.md" "docs\"
Move-IfExists "query_descriptions.txt" "docs\"
Move-IfExists "todo.txt" "docs\"

Move-IfExists "split_parquet.bat" "scripts\"

Write-Host "Creating Python package files..."

New-Item "src\__init__.py" -ItemType File -Force | Out-Null
New-Item "src\utils\__init__.py" -ItemType File -Force | Out-Null

@'
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
PARQUET_DIR = PROJECT_ROOT / "data" / "parquet_out"
CONFIG_DIR = PROJECT_ROOT / "config"
DOCS_DIR = PROJECT_ROOT / "docs"

DEFAULT_DATA_FILE = PARQUET_DIR / "data.parquet"
'@ | Set-Content "src\utils\paths.py"

Write-Host "Patching hardcoded paths..."

$pyFiles = Get-ChildItem -Path "src" -Recurse -Filter "*.py"

foreach ($file in $pyFiles) {
    $text = Get-Content $file.FullName -Raw

    $text = $text -replace 'Path\(__file__\)\.parent / "parquet_out" / "data\.parquet"', 'DEFAULT_DATA_FILE'
    $text = $text -replace 'Path\(__file__\)\.parent / "data\.parquet"', 'DEFAULT_DATA_FILE'
    $text = $text -replace 'Path\("data\.parquet"\)\.resolve\(\)', 'DEFAULT_DATA_FILE'
    $text = $text -replace 'Path\(__file__\)\.parent / "parquet_out"', 'PARQUET_DIR'
    $text = $text -replace 'Path\("parquet_out"\)', 'PARQUET_DIR'
    $text = $text -replace '"parquet_out/', '"data/parquet_out/'
    $text = $text -replace "'parquet_out/", "'data/parquet_out/"
    $text = $text -replace '"data\.parquet"', 'str(DEFAULT_DATA_FILE)'
    $text = $text -replace "'data\.parquet'", 'str(DEFAULT_DATA_FILE)'

    if ($text -match 'DEFAULT_DATA_FILE|PARQUET_DIR|RAW_DATA_DIR|CONFIG_DIR') {
        if ($text -notmatch 'from src\.utils\.paths import') {
            if ($text -match 'from pathlib import Path') {
                $text = $text -replace 'from pathlib import Path\r?\n', "from pathlib import Path`nfrom src.utils.paths import DEFAULT_DATA_FILE, PARQUET_DIR, RAW_DATA_DIR, CONFIG_DIR`n"
            }
            else {
                $text = "from src.utils.paths import DEFAULT_DATA_FILE, PARQUET_DIR, RAW_DATA_DIR, CONFIG_DIR`n" + $text
            }
        }
    }

    Set-Content $file.FullName $text
}

Write-Host "Patching transform.py if present..."

$transformPath = "src\converters\transform.py"

if (Test-Path $transformPath) {
@'
from src.utils.paths import RAW_DATA_DIR, PARQUET_DIR

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

csv_file_path = RAW_DATA_DIR / "sensors.csv"
parquet_file_path = PARQUET_DIR / "data.parquet"

df = pd.read_csv(csv_file_path)
table = pa.Table.from_pandas(df)

PARQUET_DIR.mkdir(parents=True, exist_ok=True)
pq.write_table(table, parquet_file_path)

print(f"Wrote parquet file to {parquet_file_path}")
'@ | Set-Content $transformPath
}

Write-Host "Updating .gitignore..."

@'
__pycache__/
*.pyc
.venv/
.env
'@ | Set-Content ".gitignore"

Write-Host "Running git status..."
git status

if ($Commit) {
    Write-Host "Committing changes..."

    $branchName = "refactor-project-organization"

    git switch -c $branchName
    git add .
    git commit -m "Refactor senior project organization"
}

if ($Push) {
    Write-Host "Pushing branch..."
    git push -u origin refactor-project-organization
}

Write-Host "Done."