import re
import sqlparse
import sys
from sqlparse.sql import Identifier
from datetime import datetime
import traceback

__all__ = [
    'convert_tsql_to_databricks',
    'fix_sql_formatting',
    'fix_backticks',
    'process_sql_file'
]

def is_dbt_model(content):
    # Look for DBT config block
    dbt_pattern = r'\{\{\s*config\s*\((.*?)\)\s*\}\}'
    match = re.search(dbt_pattern, content, re.DOTALL)
    return match


def remove_nolock_hint(sql):
    return re.sub(r'\bWITH\s*\(\s*NOLOCK\s*\)', '', sql, flags=re.IGNORECASE)


def update_dbt_config(header_match):
    """Update DBT config block with only essential parameters"""
    config_content = header_match.group(1)
    params = []
    
    # Define allowed parameters and their default values
    allowed_params = {
        'unique_key': None,
        'alias': None,
        'materialized': None,
        'enabled': None,
        'pre_hook': None,
        'post_hook': None
    }
    
    # Extract existing parameters
    for param in re.finditer(r'(\w+)\s*=\s*([^,\n\)]+)', config_content):
        key, value = param.groups()
        if key not in allowed_params:
            continue
            
        # Clean up the value
        value = value.strip().strip("'").strip()
        
        # Format based on value type
        if value.lower() in ('true', 'false'):
            params.append(f"        {key}={value}")
        else:
            params.append(f"        {key}='{value}'")
    
    # Join with comma at start of new line
    joined_params = "\n".join(params)
    if len(params) > 1:  # Only add commas if there's more than one parameter
        joined_params = joined_params.replace("\n        ", "\n        ,")
    
    # Construct config block with proper formatting
    return "{{\n    config(\n" + joined_params + "\n    )\n}}"




def convert_data_types(sql):
    # Handle CONVERT statements from most specific to most generic
    patterns = [
        # Pattern 1: CONVERT with multiple COALESCE concatenations
        (r'CONVERT\s*\(\s*VARCHAR\s*\(\d+\)\s*,\s*COALESCE\s*\(\s*CONVERT\s*\(\s*NVARCHAR\s*\(\d+\)\s*,\s*([^,)]+)\s*\)\s*,\s*\'([^\']+)\'\s*\)\s*\+\s*\'\|\'\s*\+\s*COALESCE\s*\(\s*CONVERT\s*\(\s*NVARCHAR\s*\(\d+\)\s*,\s*([^,)]+)\s*\)\s*,\s*\'([^\']+)\'\s*\)\s*\)',
         lambda m: f"cast(coalesce(cast({m.group(1)} as string), '{m.group(2)}') || '|' || coalesce(cast({m.group(3)} as string), '{m.group(4)}') as string)",
         re.IGNORECASE),
        
        # Pattern 2: CONVERT with string concatenation and COALESCE
        (r'CONVERT\s*\(\s*VARCHAR\s*\(\d+\)\s*,\s*\'([^\']+)\'\s*\+\s*\'\|\'\s*\+\s*COALESCE\s*\(\s*CONVERT\s*\(\s*NVARCHAR\s*\(\d+\)\s*,\s*([^,)]+)\s*\)\s*,\s*\'([^\']+)\'\s*\)\s*\)',
         lambda m: f"cast('{m.group(1)}' || '|' || coalesce(cast({m.group(2)} as string), '{m.group(3)}') as string)",
         re.IGNORECASE),
        
        # Pattern 3: Simple CONVERT with nested COALESCE
        (r'CONVERT\s*\(\s*VARCHAR\s*\(\d+\)\s*,\s*COALESCE\s*\(\s*CONVERT\s*\(\s*NVARCHAR\s*\(\d+\)\s*,\s*([^,)]+)\s*\)\s*,\s*\'([^\']+)\'\s*\)\s*\)',
         lambda m: f"cast(coalesce(cast({m.group(1)} as string), '{m.group(2)}') as string)",
         re.IGNORECASE),
        
        # Pattern 4: CONVERT binary
        (r'convert\s*\(\s*binary\s*\(\s*(\d+)\s*\)\s*,\s*([^)]+)\)',
         lambda m: f"cast({m.group(2)} as binary({m.group(1)}))",
         re.IGNORECASE),
        
        # Pattern 5: CONVERT datetime2
        (r'convert\s*\(\s*datetime2\s*\(\s*\d+\s*\)\s*,\s*([^)]+)\)',
         lambda m: f"cast({m.group(1)} as timestamp)",
         re.IGNORECASE),
        
        # Pattern 6: CAST as BIT
        (r'cast\s*\(\s*(\d+)\s*as\s*bit\s*\)',
         lambda m: f"cast({m.group(1)} as boolean)",
         re.IGNORECASE),
        
        # Pattern 7: Generic CONVERT (catch-all)
        (r'CONVERT\s*\(\s*(?:N?VARCHAR)\s*\([^)]+\)\s*,\s*([^)]+)\)',
         lambda m: f"cast({m.group(1)} as string)",
         re.IGNORECASE),
    ]
    
    # Apply all patterns in order
    for pattern, replacement, flags in patterns:
        sql = re.sub(pattern, replacement, sql, flags=flags)
    
    # Handle type declarations last
    type_conversions = [
        # VARCHAR/NVARCHAR to string
        (r'(?:n?varchar)\s*\(\s*(?:max|\d+)\s*\)', 'string'),
        # TINYINT to INT
        (r'\btinyint\b', 'int')
    ]
    
    for pattern, replacement in type_conversions:
        sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)
    
    return sql


def convert_window_functions(sql):
    return sql

def convert_dbt_vars(sql):
    sql = re.sub(r'{% if (.+?) %}', r'{% if \1 %}', sql)
    sql = re.sub(r'{% elif (.+?) %}', r'{% elif \1 %}', sql)
    sql = re.sub(r'{% else %}', r'{% else %}', sql)
    sql = re.sub(r'{% endif %}', r'{% endif %}', sql)
    sql = re.sub(r'SYSDATETIME\(\)', 'current_timestamp()', sql, flags=re.IGNORECASE)
    sql = re.sub(r'GETDATE\(\)', 'current_timestamp()', sql, flags=re.IGNORECASE)
    return sql



# working 1
def convert_equal_alias_to_as(sql):
    """Convert = style aliases to AS syntax, excluding complex expressions"""
    
    def handle_complex_expression(match):
        prefix = match.group(1) or ''  # Handle None case for first column
        alias = match.group(2)
        expr = match.group(3)
        return f'{prefix}{expr} AS {alias}'
    
    patterns = [
        # First column after SELECT (no comma)
        (r'(SELECT\s+(?:DISTINCT\s+)?)\[?([A-Za-z_]\w*)\]?\s*=\s*([A-Za-z_]\w*\.[A-Za-z_]\w*)', 
         lambda m: f'{m.group(1)}{m.group(3)} AS {m.group(2)}'),
        
        # Complex FLOOR/DATEDIFF pattern with whitespace preservation
        (r'(\s*,\s*)([A-Za-z_]\w*)\s*=\s*(FLOOR\s*\(\s*DATEDIFF\s*\([^)]*\)[^)]*\))', 
         lambda m: f'{m.group(1)}{m.group(3)} AS {m.group(2)}'),
        
        # Other patterns only match if not already processed
        (r'(\s*,\s*)([A-Za-z_]\w*)\s*=\s*(CONCAT\([^)]+\))', 
         lambda m: f'{m.group(1)}{m.group(3)} AS {m.group(2)}'),
        
        # CASE statement alias
        (r'(\s*,\s*)\[?([A-Za-z_]\w*)\]?\s*=\s*(CASE\b.*?END)', 
         lambda m: f'{m.group(1)}{m.group(3)} AS {m.group(2)}'),
        
        # Multi-line concatenation pattern
        (r'(\s*,\s*)\[?([A-Za-z_]\w*)\]?\s*=\s*(COALESCE\([^)]+\)(?:\s*[\+\|]{2}\s*(?:\r?\n\s*)?COALESCE\([^)]+\))*)', 
         lambda m: m.group(1) + ' || '.join(part.strip() for part in re.split(r'\s*[\+\|]{2}\s*', m.group(3))) + f' AS {m.group(2)}'),
        
        # Table qualified column reference
        (r'(\s*,\s*)([A-Za-z_]\w*)\s*=\s*([A-Za-z_]\w*)\s*\.\s*([A-Za-z_]\w*)', 
         lambda m: f'{m.group(1)}{m.group(3)}.{m.group(4)} AS {m.group(2)}'),
        
        # Function with alias before
        (r'(\s*,\s*)\[?([A-Za-z_]\w*)\]?\s*=\s*(LEFT|COALESCE|CONVERT|ISNULL)\s*\((.*?)\)', 
         lambda m: f'{m.group(1)}{m.group(3)}({m.group(4)}) AS {m.group(2)}'),
        
        # Simple column alias with brackets (must come last)
        (r'(\s*,\s*)\[?([A-Za-z_]\w*)\]?\s*=\s*([^,\n]+)', 
         lambda m: f'{m.group(1)}{m.group(3)} AS {m.group(2)}'),
        
        # Handle remaining equals with backticks
        (r'(\s*,\s*)(`[^`]+`)\s*=\s*(`[^`]+`)', 
         lambda m: f'{m.group(1)}{m.group(2)} AS {m.group(3)}'),
        
        # Handle any remaining equals between identifiers
        (r'(\s*,\s*)([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)\s*=\s*([A-Za-z_][A-Za-z0-9_]*)', 
         lambda m: f'{m.group(1)}{m.group(2)} AS {m.group(3)}'),
        
        # Handle function calls with equals
        (r'(\s*,\s*)(UPPER|LOWER|TRIM|CAST|CONVERT|COALESCE|LEFT|RIGHT)\s*\([^)]+\)\s*=\s*([A-Za-z_][A-Za-z0-9_]*)', 
         lambda m: f'{m.group(1)}{m.group(2)} AS {m.group(3)}'),
        
        # Handle reverse order (alias = expression)
        (r'(\s*,\s*)([A-Za-z_][A-Za-z0-9_]*)\s*=\s*([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)', 
         lambda m: f'{m.group(1)}{m.group(3)} AS {m.group(2)}')
    ]
    
    for pattern, replacement in patterns:
        sql = re.sub(pattern, replacement, sql, flags=re.DOTALL | re.IGNORECASE)
    
    return sql



def convert_concatenation(sql):
    # Replace '+' with '||' when not within single quotes
    sql = re.sub(r'(?<!\')\s*\+\s*(?!\')', ' || ', sql)
    return sql


def convert_brackets(sql):
    # Convert square brackets to backticks, but not within DBT tags or config blocks
    in_dbt = False
    result = ""
    i = 0
    
    while i < len(sql):
        if sql[i:i+2] == '{{':
            in_dbt = True
            result += sql[i:i+2]
            i += 2
        elif sql[i:i+2] == '}}':
            in_dbt = False
            result += sql[i:i+2]
            i += 2
        elif not in_dbt and sql[i] == '[':
            result += '`'
            i += 1
        elif not in_dbt and sql[i] == ']':
            result += '`'
            i += 1
        else:
            result += sql[i]
            i += 1
            
    return result





def convert_cast(sql):
    sql = re.sub(r'CONVERT\s*\(\s*(NVARCHAR)\s*,\s*(.+?)\s*\)', r'CAST(\2 AS STRING)', sql, flags=re.IGNORECASE)
    sql = re.sub(r'CONVERT\s*\(\s*(\w+)\s*,\s*(.+?)\s*\)', r'CAST(\2 AS \1)', sql, flags=re.IGNORECASE)
    return sql


def convert_isnull(sql):
     # Convert all ISNULL() functions to COALESCE()
    sql = re.sub(r'ISNULL\s*\(', 'COALESCE(', sql, flags=re.IGNORECASE)
    return sql


def convert_numeric(sql):
     # Convert all ISNULL() functions to COALESCE()
    sql = re.sub(r'NUMERIC\s*\(', 'DECIMAL(', sql, flags=re.IGNORECASE)
    return sql

def convert_hash_functions(sql):
    # Update the hash function mapping
    hash_func_map = {
        'SHA2_256': 'sha2',
        'SHA2_512': 'sha512',
        'MD5': 'md5',
        'SHA1': 'sha1'
    }
    
    # Handle HASHBYTES with CONVERT
    pattern = r'CONVERT\s*\(\s*BINARY\s*\(\s*\d+\s*\)\s*,\s*HASHBYTES\s*\(\s*\'([^\']+)\'\s*,\s*([^)]+)\)\s*\)'
    sql = re.sub(pattern, 
                lambda m: f'CAST({hash_func_map[m.group(1).strip().upper()]}({m.group(2)}) AS BINARY)',
                sql, 
                flags=re.IGNORECASE)
    
    # Handle regular HASHBYTES
    pattern = r'HASHBYTES\s*\(\s*\'([^\']+)\'\s*,\s*([^)]+)\)'
    sql = re.sub(pattern, 
                lambda m: f'{hash_func_map[m.group(1).strip().upper()]}({m.group(2)})',
                sql, 
                flags=re.IGNORECASE)
    
    return sql




def process_unconverted(parsed):
    new_parsed = []
    for stmt in parsed:
        if stmt.get_type() in ('UNKNOWN', 'DDL'):
            stmt_str = str(stmt)
            if re.search(r'{% if(.+?)%}', stmt_str, flags=re.IGNORECASE):
                new_parsed.append(f'{stmt_str}\n')
            else:
                new_parsed.append(f'-- Unable to convert:\n-- {stmt_str}\n')
        else:
            new_parsed.append(f'{stmt}\n')
    return new_parsed


def move_alias_in_case_statements(content):
    def replace_case_alias(match):
        alias = match.group(1)
        case_stmt = match.group(2)
        # Clean up any incorrect AS placements
        case_stmt = re.sub(r'THEN\s+([^\s]+)\s+AS\s+ALIAS', r'THEN \1', case_stmt)
        return f"{case_stmt} AS {alias}"
    
    # Handle cases where alias is before the CASE statement
    pattern = r'(\w+)\s*=\s*(CASE\b.*?END)'
    content = re.sub(pattern, replace_case_alias, content, flags=re.DOTALL | re.IGNORECASE)
    
    return content

def fix_join_conditions(content):
    def fix_condition(match):
        condition = match.group(0)
        # Remove incorrect AS clauses in join conditions
        condition = re.sub(r'(\w+\.\w+)\s+AS\s+(\w+)', r'\1 = \2', condition)
        return condition
    
    pattern = r'ON\s+.*?(?=(JOIN|\s*$))'
    content = re.sub(pattern, fix_condition, content, flags=re.DOTALL | re.IGNORECASE)
    return content

def fix_backticks(content):
    """Ensure consistent backtick usage while preserving Jinja and handling SQL within Jinja blocks"""
    
    def process_jinja_sql(match):
        """Process SQL within Jinja blocks separately"""
        jinja_content = match.group(0)
        
        # Extract SQL part from Jinja
        sql_match = re.search(r'SELECT.*?(?={%-?\s*end)', jinja_content, re.DOTALL | re.IGNORECASE)
        if sql_match:
            sql_part = sql_match.group(0)
            # Process SQL normally
            processed_sql = fix_sql_identifiers(sql_part)
            # Replace original SQL with processed version
            return jinja_content.replace(sql_part, processed_sql)
        return jinja_content
    
    def fix_sql_identifiers(sql):
        """Fix backticks for SQL identifiers only"""
        # SQL keywords that should not be backticked
        keywords = set(['SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'AS', 'IN', 'ON', 'JOIN'])
        
        def replace_identifier(match):
            word = match.group(0)
            if word.upper() in keywords:
                return word
            return word  # Don't add backticks within Jinja SQL
            
        return re.sub(r'\b[A-Za-z_][A-Za-z0-9_.]*\b', replace_identifier, sql)
    
    # Process Jinja blocks first
    content = re.sub(r'\{%-?\s*call.*?endcall\s*-?%\}', process_jinja_sql, content, flags=re.DOTALL)
    
    # Process remaining SQL normally
    # ... rest of the backtick processing for non-Jinja SQL ...
    
    return content

def cleanup_unconverted_equals(sql):
    """Second pass to clean up any remaining equals that should be AS"""
    
    patterns = [
        # Handle table.column = alias pattern
        (r',\s*(`[^`]+`\.`[^`]+`)\s*=\s*(`[^`]+`)', r',\1 AS \2'),
        
        # Handle remaining equals with backticks
        (r',\s*(`[^`]+`)\s*=\s*(`[^`]+`)', r',\1 AS \2'),
        
        # Handle any remaining equals between identifiers
        (r',\s*([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)\s*=\s*([A-Za-z_][A-Za-z0-9_]*)', r',\1 AS \2'),
        
        # Handle function calls with equals
        (r',\s*(UPPER|LOWER|TRIM|CAST|CONVERT|COALESCE|LEFT|RIGHT)\s*\([^)]+\)\s*=\s*([A-Za-z_][A-Za-z0-9_]*)', r',\1 AS \2'),
        
        # Handle reverse order (alias = expression)
        (r',\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)', r',\2 AS \1')
    ]
    
    for pattern, replacement in patterns:
        sql = re.sub(pattern, replacement, sql, flags=re.DOTALL | re.IGNORECASE)
    
    return sql

def fix_column_aliases(content):
    """Fix column alias syntax for complex expressions"""
    
    def handle_complex_expression(match):
        full_expr = match.group(0)
        alias = match.group(1)
        expression = match.group(2)
        
        # Remove the alias from the start
        # Add it to the end of the expression
        return f',{expression} AS {alias}'
    
    # Pattern for complex expressions with alias at start
    complex_pattern = r',\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*((?:FLOOR|CEILING|ROUND|ABS)\s*\([^)]*(?:\([^)]*\)[^)]*)*\))'
    
    content = re.sub(complex_pattern, handle_complex_expression, content, flags=re.DOTALL | re.IGNORECASE)
    
    return content

def convert_brackets_and_quotes(sql):
    # First convert square brackets to temporary marker
    sql = re.sub(r'\[([^\]]+)\]', r'__TEMP_BRACKET__\1__TEMP_BRACKET__', sql)
    
    # Convert double quotes to temporary marker (but not within DBT tags)
    in_dbt = False
    result = ""
    i = 0
    
    while i < len(sql):
        if sql[i:i+2] == '{{':
            in_dbt = True
            result += sql[i:i+2]
            i += 2
        elif sql[i:i+2] == '}}':
            in_dbt = False
            result += sql[i:i+2]
            i += 2
        elif not in_dbt and sql[i] == '"':
            result += '__TEMP_QUOTE__'
            i += 1
        else:
            result += sql[i]
            i += 1
    
    sql = result
    
    # Now convert all temporary markers to backticks
    sql = sql.replace('__TEMP_BRACKET__', '`')
    sql = sql.replace('__TEMP_QUOTE__', '`')
    
    return sql

def convert_tsql_to_databricks(file_path, output_path):
    with open(file_path, 'r') as file:
        content = file.read()

    # Add headers
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    header = f'-- Converted on: {now}\n'
    header += f'-- Command: python {" ".join(sys.argv)}\n\n'
    
    # First handle DBT config separately
    dbt_header_match = is_dbt_model(content)
    if dbt_header_match:
        dbt_config = update_dbt_config(dbt_header_match)
        # Remove the original config block
        content = re.sub(re.escape(dbt_header_match.group(0)), '', content, count=1)
    
    # Apply transformations in correct order
    content = convert_concatenation(content)
    content = convert_equal_alias_to_as(content)
    content = convert_brackets_and_quotes(content)
    content = move_alias_in_case_statements(content)
    content = fix_join_conditions(content)
    content = convert_window_functions(content)
    content = convert_dbt_vars(content)
    content = convert_cast(content)
    content = convert_hash_functions(content)
    content = remove_nolock_hint(content)
    content = convert_data_types(content)
    content = convert_brackets(content)
    content = convert_isnull(content)
    content = convert_numeric(content)
    content = fix_backticks(content)
    
    # Add cleanup pass
    content = cleanup_unconverted_equals(content)
    
    # Add back the DBT config at the start if it existed
    if dbt_header_match:
        content = dbt_config + '\n\n' + content

    # Write the converted content
    with open(output_path, 'w') as output_file:
        output_file.write(header + content)

def process_sql_file(input_path, output_path):
    """Process a single SQL file with proper error handling"""
    try:
        print(f"Reading file: {input_path}")
        with open(input_path, 'r') as file:
            content = file.read()
        
        # Order matters: formatting -> backticks
        print("Fixing SQL formatting...")
        content = fix_sql_formatting(content)
        
        print("Fixing backticks...")
        content = fix_backticks(content)
        
        # Write to temporary file
        temp_path = f"{input_path}.tmp"
        print(f"Writing temp file: {temp_path}")
        with open(temp_path, 'w') as temp:
            temp.write(content)
            
        print("Converting using main script...")
        convert_tsql_to_databricks(temp_path, output_path)
        
        # Clean up temp file
        os.remove(temp_path)
        print(f"Successfully converted {input_path}")
    except Exception as e:
        print(f"Error converting {input_path}")
        print(f"Error details: {str(e)}")
        print(f"Error type: {type(e)}")
        import traceback
        trace = traceback.format_exc()
        formatted_trace = trace.replace('\n', '\n-- ')
        with open(output_path, 'w') as f:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f'-- Error during conversion on: {now}\n')
            f.write(f'-- Error: {str(e)}\n')
            f.write(f'-- Stack trace:\n-- {formatted_trace}\n')
            f.write(f'-- Original file: {input_path}\n')

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: python3 tsql_to_databricks.py input_file.sql output_file.sql")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]

    convert_tsql_to_databricks(input_file, output_file)
