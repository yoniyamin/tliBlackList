import csv
import json
import os
import io
import argparse
import sys
from flask import Flask, render_template, request, redirect, url_for, send_file, after_this_request
from werkzeug.utils import secure_filename

app = Flask(__name__)

UPLOAD_FOLDER = 'uploads'
RESULT_FOLDER = 'results'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['RESULT_FOLDER'] = RESULT_FOLDER

# Ensure the upload and results folders exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULT_FOLDER, exist_ok=True)

def load_csv_as_list_of_dicts(csv_file_path, has_headers):
    """
    Read a two-column CSV (schema, table_name). If has_headers=True, uses DictReader
    (first row is treated as column names). Otherwise expects exactly two columns
    per line with no header row.
    Returns a list of {"owner": ..., "name": ...}.
    """
    rows = []
    with open(csv_file_path, newline='', encoding='utf-8') as f:
        if has_headers:
            reader = csv.DictReader(f)
            headers = [h.strip() for h in reader.fieldnames]
            if len(headers) < 2:
                raise ValueError(
                    f"Expected at least two columns in {csv_file_path}, found: {headers}"
                )
            for r in reader:
                owner = r[headers[0]].strip()
                name = r[headers[1]].strip()
                if owner and name:
                    rows.append({"owner": owner, "name": name})
        else:
            reader = csv.reader(f)
            for r in reader:
                if len(r) < 2:
                    continue
                owner = r[0].strip()
                name = r[1].strip()
                if owner and name:
                    rows.append({"owner": owner, "name": name})
    return rows

def parse_pattern(pattern_str):
    """Parse a pattern like 'public.%' into owner and name parts."""
    if "." not in pattern_str:
        raise ValueError(f"Pattern must be of the form <schema>.<table_pattern>, got '{pattern_str}'")
    owner_part, name_part = pattern_str.split(".", 1)
    return {"owner": owner_part.strip(), "name": name_part.strip()}

def parse_manual_patterns(text):
    """Parse manual text input into list of patterns/tables."""
    patterns = []
    for line in text.strip().split('\n'):
        line = line.strip()
        if line:
            try:
                pattern = parse_pattern(line)
                patterns.append(pattern)
            except ValueError:
                # If it's not a valid pattern, skip it
                continue
    return patterns

def create_task_json(csv_file_path=None, csv_include_headers=True,
                    include_pattern=None, exclude_patterns_text=None, exclude_csv_path=None,
                    csv_exclude_headers=True, json_input_path=None, merge_action=True):
    """
    Create or modify a replication task JSON with the provided configuration.
    """
    # Initialize data structure
    if json_input_path:
        with open(json_input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    else:
        # Create a basic task structure
        data = {
            "cmd.replication_definition": {
                "tasks": [{
                    "source": {
                        "source_tables": {
                            "included_pattern": [],
                            "excluded_pattern": [],
                            "explicit_included_tables": []
                        }
                    }
                }]
            }
        }

    # Get reference to source_tables
    source_tables = data["cmd.replication_definition"]["tasks"][0]["source"]["source_tables"]

    # Ensure all required sections exist
    if "included_pattern" not in source_tables:
        source_tables["included_pattern"] = []
    if "excluded_pattern" not in source_tables:
        source_tables["excluded_pattern"] = []
    if "explicit_included_tables" not in source_tables:
        source_tables["explicit_included_tables"] = []

    # Process CSV file if provided (always goes to explicit_included_tables)
    if csv_file_path:
        csv_data = load_csv_as_list_of_dicts(csv_file_path, csv_include_headers)
        
        if not merge_action:
            source_tables["explicit_included_tables"] = csv_data
        else:
            source_tables["explicit_included_tables"].extend(csv_data)

    # Process include pattern
    if include_pattern and include_pattern.strip():
        try:
            pattern = parse_pattern(include_pattern.strip())
            if not merge_action:
                source_tables["included_pattern"] = [pattern]
            else:
                source_tables["included_pattern"].append(pattern)
        except ValueError as e:
            print(f"Warning: Invalid include pattern: {e}")

    # Process exclude patterns (manual text)
    if exclude_patterns_text and exclude_patterns_text.strip():
        exclude_data = parse_manual_patterns(exclude_patterns_text)
        # Add load_order for excluded tables
        excluded_data = [{"owner": item["owner"], "name": item["name"], "load_order": -1} 
                        for item in exclude_data]
        if not merge_action:
            source_tables["excluded_pattern"] = excluded_data
        else:
            source_tables["excluded_pattern"].extend(excluded_data)

    # Process exclude CSV
    if exclude_csv_path:
        exclude_csv_data = load_csv_as_list_of_dicts(exclude_csv_path, csv_exclude_headers)
        excluded_data = [{"owner": item["owner"], "name": item["name"], "load_order": -1} 
                        for item in exclude_csv_data]
        if not merge_action:
            source_tables["excluded_pattern"] = excluded_data
        else:
            source_tables["excluded_pattern"].extend(excluded_data)

    return data

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/convert', methods=['POST'])
def convert():
    try:
        # Get form data
        csv_file = request.files.get('csv_file')
        json_input_file = request.files.get('json_input_file')
        exclude_csv_file = request.files.get('exclude_csv_file')
        
        csv_include_headers = 'csv_include_headers' in request.form
        csv_exclude_headers = 'csv_exclude_headers' in request.form
        include_pattern = request.form.get('include_pattern', '').strip()
        exclude_patterns_text = request.form.get('exclude_patterns', '').strip()
        exclude_input_type = 'exclude_input_type' in request.form  # True if CSV, False if manual
        merge_action = 'merge_action' in request.form
        
        # Track files to clean up
        files_to_cleanup = []
        
        # Handle CSV file upload
        csv_path = None
        if csv_file and csv_file.filename != '':
            csv_filename = secure_filename(csv_file.filename)
            csv_path = os.path.join(app.config['UPLOAD_FOLDER'], csv_filename)
            csv_file.save(csv_path)
            files_to_cleanup.append(csv_path)

        # Handle JSON input file
        json_input_path = None
        if json_input_file and json_input_file.filename != '':
            json_input_filename = secure_filename(json_input_file.filename)
            json_input_path = os.path.join(app.config['UPLOAD_FOLDER'], json_input_filename)
            json_input_file.save(json_input_path)
            files_to_cleanup.append(json_input_path)

        # Handle exclude CSV file
        exclude_csv_path = None
        if exclude_input_type and exclude_csv_file and exclude_csv_file.filename != '':
            exclude_csv_filename = secure_filename(exclude_csv_file.filename)
            exclude_csv_path = os.path.join(app.config['UPLOAD_FOLDER'], exclude_csv_filename)
            exclude_csv_file.save(exclude_csv_path)
            files_to_cleanup.append(exclude_csv_path)

        # Validate that at least one input method is provided
        has_csv = csv_path is not None
        has_include_pattern = include_pattern != ''
        has_exclude_manual = exclude_patterns_text != '' and not exclude_input_type
        has_exclude_csv = exclude_csv_path is not None
        
        if not (has_csv or has_include_pattern or has_exclude_manual or has_exclude_csv):
            return "Error: Please provide at least one input (CSV file, include pattern, or exclude patterns/tables).", 400

        # Set output file path
        json_output_file_path = os.path.join(app.config['RESULT_FOLDER'], 'replication_task.json')

        # Create the task JSON
        data = create_task_json(
            csv_file_path=csv_path,
            csv_include_headers=csv_include_headers,
            include_pattern=include_pattern,
            exclude_patterns_text=exclude_patterns_text if not exclude_input_type else None,
            exclude_csv_path=exclude_csv_path,
            csv_exclude_headers=csv_exclude_headers,
            json_input_path=json_input_path,
            merge_action=merge_action
        )

        # Write the output JSON
        with open(json_output_file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)

        # Clean up uploaded files after sending the response
        @after_this_request
        def remove_files(response):
            for file_path in files_to_cleanup:
                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                except Exception as e:
                    print(f"Error deleting file {file_path}: {e}")
            return response

        # Send the resulting JSON file to the user
        return send_file(json_output_file_path, as_attachment=True, download_name='replication_task.json')

    except Exception as e:
        return f"Error processing request: {str(e)}", 500

def main_cli():
    """CLI interface for the Qlik Replicate Task Manager."""
    parser = argparse.ArgumentParser(
        description="Qlik Replicate Task Manager - Manage table capture lists, exclusions, and patterns"
    )
    
    # Input options
    parser.add_argument(
        "--csv-include",
        help="CSV file with specific tables to include (schema,table_name format)"
    )
    parser.add_argument(
        "--csv-include-headers",
        action="store_true",
        default=True,
        help="CSV include file has headers (default: True)"
    )
    parser.add_argument(
        "--include-pattern",
        help="Include pattern for whitelist (e.g., 'public.%%', 'dbo.user_%%')"
    )
    parser.add_argument(
        "--exclude-patterns",
        help="Text file with exclude patterns/tables (one per line, schema.table format)"
    )
    parser.add_argument(
        "--exclude-csv",
        help="CSV file with exclude patterns/tables (schema,table_name format)"
    )
    parser.add_argument(
        "--csv-exclude-headers",
        action="store_true",
        default=True,
        help="CSV exclude file has headers (default: True)"
    )
    
    # JSON input/output
    parser.add_argument(
        "--json-input",
        help="Existing task JSON file to modify"
    )
    parser.add_argument(
        "--json-output",
        required=True,
        help="Output path for the generated/modified task JSON"
    )
    
    # Action
    parser.add_argument(
        "--action",
        choices=["merge", "replace"],
        default="merge",
        help="Action when modifying existing JSON: merge or replace (default: merge)"
    )
    
    args = parser.parse_args()
    
    # Validate that at least one input is provided
    inputs_provided = [
        args.csv_include,
        args.include_pattern,
        args.exclude_patterns,
        args.exclude_csv
    ]
    
    if not any(inputs_provided):
        print("Error: At least one input option must be provided", file=sys.stderr)
        print("Use --help for available options", file=sys.stderr)
        sys.exit(1)
    
    try:
        # Read exclude patterns from text file if provided
        exclude_patterns_text = None
        if args.exclude_patterns:
            with open(args.exclude_patterns, 'r', encoding='utf-8') as f:
                exclude_patterns_text = f.read().strip()
        
        # Create task JSON
        data = create_task_json(
            csv_file_path=args.csv_include,
            csv_include_headers=args.csv_include_headers,
            include_pattern=args.include_pattern,
            exclude_patterns_text=exclude_patterns_text,
            exclude_csv_path=args.exclude_csv,
            csv_exclude_headers=args.csv_exclude_headers,
            json_input_path=args.json_input,
            merge_action=(args.action == "merge")
        )
        
        # Write output JSON
        with open(args.json_output, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        
        print(f"✅ Successfully created task configuration: {args.json_output}")
        
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] not in ['run', 'flask']:
        # CLI mode
        main_cli()
    else:
        # Web mode
        app.run(debug=True)
