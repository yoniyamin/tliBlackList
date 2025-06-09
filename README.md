# Qlik Replicate Task Manager

A web and CLI tool for managing Qlik Replicate tasks, specifically for configuring table capture lists, exclusions, and patterns.

## Description

This tool helps you create and modify Qlik Replicate task configurations by providing an easy way to:
- Include specific tables via CSV upload
- Define include patterns (whitelist) for capturing tables
- Specify exclude patterns/tables (blacklist) via manual entry or CSV upload
- Modify existing task JSON configurations

## Features

- **Web Interface**: User-friendly web interface for creating and modifying task configurations
- **CLI Support**: Command-line interface for automation and scripting
- **Multiple Input Methods**:
  - CSV files for specific tables to include or exclude
  - Pattern-based inclusion and exclusion
  - Manual entry of exclusion patterns
- **Flexible Configuration**:
  - Create new task configurations
  - Modify existing task configurations with merge or replace options

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/tliweb.git
   cd tliweb
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

## Usage

### Web Interface

1. Start the web server:
   ```
   python tliweb.py
   ```

2. Open your browser and navigate to `http://localhost:5000`

3. Use the web interface to:
   - Upload CSV files with tables to include or exclude
   - Specify include patterns for whitelisting
   - Specify exclude patterns for blacklisting
   - Modify existing JSON task configurations

4. Click "Generate Task Configuration" to download the resulting JSON file

### Command Line Interface

The tool also provides a CLI for automation and scripting:

```
python tliweb.py --csv-include <csv_file> --include-pattern <pattern> --json-output <output_file>
```

#### CLI Options:

- `--csv-include`: CSV file with specific tables to include (schema,table_name format)
- `--csv-include-headers`: CSV include file has headers (default: True)
- `--include-pattern`: Include pattern for whitelist (e.g., 'public.%', 'dbo.user_%')
- `--exclude-patterns`: Text file with exclude patterns/tables (one per line, schema.table format)
- `--exclude-csv`: CSV file with exclude patterns/tables (schema,table_name format)
- `--csv-exclude-headers`: CSV exclude file has headers (default: True)
- `--json-input`: Existing task JSON file to modify
- `--json-output`: Output path for the generated/modified task JSON
- `--action`: Action when modifying existing JSON: merge or replace (default: merge)

## Examples

### Include all tables from the 'public' schema except temporary tables:

```
python tliweb.py --include-pattern "public.%" --exclude-patterns exclude_list.txt --json-output task.json
```

Where `exclude_list.txt` contains:
```
public.temp_%
public.log_table
```

### Include specific tables from a CSV file:

```
python tliweb.py --csv-include tables.csv --json-output task.json
```

Where `tables.csv` contains:
```
schema,table
public,users
public,orders
dbo,customers
```

## Author

Yonatan Yamin
