#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Add author_cbdb_ids field to markdown files in data directory.

This script:
1. Parses markdown files to extract author field
2. Queries CBDB API to find matching person IDs
3. Adds author_cbdb_ids field after author field in frontmatter
"""

import os
import re
import time
import json
import requests
import signal
import sys
from pathlib import Path
from typing import List, Optional, Dict, Tuple
from datetime import datetime, timedelta
import argparse

# Global cache for author name -> CBDB query results
# Key: author_name (str)
# Value: tuple of (person_ids: List[int], person_details: List[Dict])
_author_cbdb_cache: Dict[str, Tuple[List[int], List[Dict]]] = {}

# Global flag for graceful shutdown
_shutdown_requested = False


def signal_handler(signum, frame):
    """Handle interrupt signals (Ctrl+C) gracefully."""
    global _shutdown_requested
    if _shutdown_requested:
        print("\n\n强制退出...")
        sys.exit(1)
    print("\n\n收到中断信号，正在优雅退出... (再次按 Ctrl+C 强制退出)")
    _shutdown_requested = True


def extract_author_name(author_field: str) -> Optional[str]:
    """
    Extract clean author name from author field.

    Examples:
        '[唐]慧光释' -> '慧光释'
        '[宋]洪兴祖撰' -> '洪兴祖'
        '[唐]慧净' -> '慧净'

    Args:
        author_field: Raw author field from frontmatter

    Returns:
        Clean author name without dynasty prefix and suffix, or None if invalid
    """
    if not author_field:
        return None

    # Remove leading/trailing whitespace and quotes
    author_field = author_field.strip().strip("'\"")

    # Pattern: [dynasty]name or [dynasty]name+suffix
    # We want to extract just the name part (keep 释 for Buddhist monks)
    match = re.match(r'\[.+?\](.+?)(?:撰|著|编|述|集)?$', author_field)
    if match:
        return match.group(1).strip()

    # If no dynasty bracket, return as-is (but remove common suffixes, keep 释)
    return re.sub(r'(撰|著|编|述|集)$', '', author_field).strip()


def is_relative_entry(name: str) -> bool:
    """
    Check if a CBDB entry is a relative (wife, mother, daughter, etc).

    Args:
        name: Chinese name from CBDB

    Returns:
        True if this appears to be a relative's entry
    """
    # Check if name contains parentheses with relative markers
    # e.g., "吳氏(湯顯祖妻)" or "王某(王逸曾祖)"
    if '(' in name or '（' in name:
        relative_markers = ['妻', '母', '女', '子', '父', '祖', '兄', '弟', '姊', '妹', '曾祖', '孫']
        return any(marker in name for marker in relative_markers)
    return False


def query_cbdb_api(author_name: str, max_retries: int = 3) -> tuple[List[int], List[Dict]]:
    """
    Query CBDB API for author name and return list of person IDs with details.
    Results are cached to avoid repeated API calls for the same author.

    Args:
        author_name: Clean author name to search
        max_retries: Maximum number of retry attempts

    Returns:
        Tuple of (person_ids, person_details) where person_details contains full info for disambiguation
    """
    if not author_name:
        return [], []

    # Check cache first
    if author_name in _author_cbdb_cache:
        print(f"    (using cached result for '{author_name}')")
        return _author_cbdb_cache[author_name]

    url = f"https://input.cbdb.fas.harvard.edu/api/name?q={author_name}"

    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            data = response.json()

            # Extract person IDs and details from data array
            person_ids = []
            person_details = []
            if 'data' in data and isinstance(data['data'], list):
                for person in data['data']:
                    if 'c_personid' in person:
                        person_id = person.get('c_personid')
                        # Skip ID 0 (未詳 - unknown)
                        if person_id == 0:
                            continue

                        person_ids.append(person_id)
                        person_details.append({
                            'id': person_id,
                            'name': person.get('c_name_chn'),
                            'dynasty': person.get('c_dynasty_chn'),
                            'year': person.get('c_index_year'),
                            'location': person.get('ADDR_c_name_chn'),
                            'is_relative': is_relative_entry(person.get('c_name_chn', ''))
                        })

            # Cache the result (even if empty)
            result = (person_ids, person_details)
            _author_cbdb_cache[author_name] = result
            return result

        except requests.exceptions.RequestException as e:
            print(f"  Warning: API request failed for '{author_name}' (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(1)  # Wait before retry

    # Cache empty result for failed queries to avoid retrying
    result = ([], [])
    _author_cbdb_cache[author_name] = result
    return result


def extract_dynasty(author_field: str) -> Optional[str]:
    """
    Extract dynasty from author field.

    Examples:
        '[唐]慧光释' -> '唐'
        '[宋]洪兴祖' -> '宋'

    Args:
        author_field: Raw author field from frontmatter

    Returns:
        Dynasty name or None if not found
    """
    match = re.match(r'\[(.+?)\]', author_field.strip().strip("'\""))
    if match:
        return match.group(1)
    return None


def process_markdown_file(file_path: Path, dry_run: bool = False, ambiguous_cases: Optional[List] = None) -> bool:
    """
    Process a single markdown file to add author_cbdb_ids field.

    Args:
        file_path: Path to markdown file
        dry_run: If True, only show what would be done without modifying files
        ambiguous_cases: List to append ambiguous cases for manual review

    Returns:
        True if file was modified (or would be modified in dry_run), False otherwise
    """
    if ambiguous_cases is None:
        ambiguous_cases = []

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return False

    # Check if file has frontmatter
    if not content.startswith('---\n'):
        return False

    # Find end of frontmatter
    end_match = re.search(r'\n---\n', content[4:])
    if not end_match:
        return False

    frontmatter_end = end_match.start() + 4
    frontmatter = content[4:frontmatter_end]
    body = content[frontmatter_end + 5:]  # +5 to skip '\n---\n'

    # Check if author_cbdb_ids already exists
    if re.search(r'^author_cbdb_ids:', frontmatter, re.MULTILINE):
        print(f"  Skipping (already has author_cbdb_ids): {file_path.relative_to(file_path.parents[2])}")
        return False

    # Extract author field
    author_match = re.search(r"^author:\s*['\"]?(.+?)['\"]?\s*$", frontmatter, re.MULTILINE)
    if not author_match:
        return False

    author_field = author_match.group(1)
    author_name = extract_author_name(author_field)
    dynasty = extract_dynasty(author_field)

    if not author_name:
        print(f"  Skipping (no valid author name): {file_path.relative_to(file_path.parents[2])}")
        return False

    # Query CBDB API
    print(f"  Querying CBDB for '{author_name}' in {file_path.relative_to(file_path.parents[2])}")
    person_ids, person_details = query_cbdb_api(author_name)

    if not person_ids:
        print(f"    No CBDB IDs found for '{author_name}'")
        # Don't modify file for empty matches
        print(f"    ⊗ Skipping file modification (no matches found)")
        return False
    elif len(person_ids) == 1:
        # Single match - straightforward
        print(f"    Found CBDB ID: {person_ids[0]}")
        detail = person_details[0]
        print(f"      {detail['name']} ({detail['dynasty']}) - {detail['location']}")
        cbdb_ids_line = f"author_cbdb_ids: {person_ids}\n"
        needs_review = False
    else:
        # Multiple matches - needs manual review
        print(f"    ⚠️  AMBIGUOUS: Found {len(person_ids)} matches for '{author_name}':")
        for detail in person_details:
            year_str = f", {detail['year']}" if detail['year'] else ""
            loc_str = f", {detail['location']}" if detail['location'] else ""
            print(f"      - ID {detail['id']}: {detail['name']} ({detail['dynasty']}{year_str}{loc_str})")

        # First, filter out relatives (妻、母、etc.)
        non_relatives = [d for d in person_details if not d.get('is_relative', False)]

        if len(non_relatives) == 1:
            # Only one non-relative match - use it
            print(f"    ✓ Single non-relative match found: Using ID {non_relatives[0]['id']}")
            cbdb_ids_line = f"author_cbdb_ids: [{non_relatives[0]['id']}]\n"
            needs_review = False
        elif len(non_relatives) > 1:
            # Multiple non-relative matches - try dynasty filter
            if dynasty:
                # Normalize dynasty for better matching (淸 -> 清)
                dynasty_normalized = dynasty.replace('淸', '清')
                filtered = [d for d in non_relatives if d['dynasty'] == dynasty or d['dynasty'] == dynasty_normalized]

                if len(filtered) == 1:
                    print(f"    ✓ Dynasty match found: Using ID {filtered[0]['id']} (dynasty: {dynasty})")
                    cbdb_ids_line = f"author_cbdb_ids: [{filtered[0]['id']}]\n"
                    needs_review = False
                elif len(filtered) > 1:
                    print(f"    ⚠️  Multiple non-relative matches for dynasty '{dynasty}'")
                    filtered_ids = [d['id'] for d in filtered]
                    cbdb_ids_line = f"author_cbdb_ids: {filtered_ids}  # NEEDS_MANUAL_REVIEW: {len(filtered)} non-relative matches for {dynasty} dynasty\n"
                    needs_review = True
                    # Update person_details to only include filtered matches for the report
                    person_details = filtered
                else:
                    print(f"    ⚠️  No non-relative matches for dynasty '{dynasty}'")
                    non_relative_ids = [d['id'] for d in non_relatives]
                    cbdb_ids_line = f"author_cbdb_ids: {non_relative_ids}  # NEEDS_MANUAL_REVIEW: {len(non_relatives)} non-relative matches, none for {dynasty} dynasty\n"
                    needs_review = True
                    # Update person_details to only include non-relatives for the report
                    person_details = non_relatives
            else:
                # No dynasty info - use all non-relatives
                non_relative_ids = [d['id'] for d in non_relatives]
                cbdb_ids_line = f"author_cbdb_ids: {non_relative_ids}  # NEEDS_MANUAL_REVIEW: {len(non_relatives)} non-relative matches\n"
                needs_review = True
                # Update person_details to only include non-relatives for the report
                person_details = non_relatives
        else:
            # No non-relatives found (all are relatives) - fall back to original logic
            print(f"    ⚠️  All matches appear to be relatives")
            cbdb_ids_line = f"author_cbdb_ids: {person_ids}  # NEEDS_MANUAL_REVIEW: all matches are relatives\n"
            needs_review = True

        # Record ambiguous case
        if needs_review:
            ambiguous_cases.append({
                'file': str(file_path.relative_to(file_path.parents[2])),
                'author_field': author_field,
                'author_name': author_name,
                'dynasty': dynasty,
                'matches': person_details
            })
            # Don't modify file for cases needing manual review
            print(f"    ⊗ Skipping file modification (needs manual review)")
            return False

    # Insert author_cbdb_ids after author field
    author_line_end = author_match.end()
    new_frontmatter = (
        frontmatter[:author_line_end] +
        '\n' + cbdb_ids_line.rstrip('\n') +
        frontmatter[author_line_end:]
    )

    new_content = '---\n' + new_frontmatter + '\n---\n' + body

    if dry_run:
        print(f"    [DRY RUN] Would add: {cbdb_ids_line.strip()}")
        return True

    # Write back to file
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"    ✓ Updated file")
        return True
    except Exception as e:
        print(f"    Error writing {file_path}: {e}")
        return False


def find_markdown_files(data_dir: Path, pattern: Optional[str] = None) -> List[Path]:
    """
    Find all markdown files in data directory that need processing.

    Uses grep to pre-filter files that:
    1. Have 'author:' field
    2. Don't already have 'author_cbdb_ids:' field

    Args:
        data_dir: Root data directory
        pattern: Optional glob pattern to filter files

    Returns:
        List of Path objects for markdown files that need processing
    """
    import subprocess

    # First, get all markdown files matching the pattern
    if pattern:
        all_files = list(data_dir.glob(pattern))
    else:
        all_files = list(data_dir.glob('**/*.md'))

    if not all_files:
        return []

    # Use grep to find files with 'author:' but without 'author_cbdb_ids:'
    # This is much faster than reading every file in Python
    try:
        # Find files with 'author:' field (case-insensitive frontmatter search)
        files_with_author = set()
        for file in all_files:
            try:
                # Use grep to check if file has 'author:' in first 50 lines (frontmatter area)
                result = subprocess.run(
                    ['grep', '-l', '-m', '1', '^author:', str(file)],
                    capture_output=True,
                    timeout=5
                )
                if result.returncode == 0:
                    files_with_author.add(file)
            except (subprocess.TimeoutExpired, Exception):
                # If grep fails, include the file to be safe
                files_with_author.add(file)

        # Find files that already have 'author_cbdb_ids:'
        files_with_cbdb_ids = set()
        for file in files_with_author:
            try:
                result = subprocess.run(
                    ['grep', '-l', '-m', '1', '^author_cbdb_ids:', str(file)],
                    capture_output=True,
                    timeout=5
                )
                if result.returncode == 0:
                    files_with_cbdb_ids.add(file)
            except (subprocess.TimeoutExpired, Exception):
                pass

        # Return files that have author but not author_cbdb_ids
        files_to_process = files_with_author - files_with_cbdb_ids
        return sorted(list(files_to_process))

    except Exception as e:
        # If grep filtering fails, fall back to returning all files
        print(f"Warning: grep filtering failed ({e}), processing all files")
        return all_files


def main():
    # Set up signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    parser = argparse.ArgumentParser(
        description='Add author_cbdb_ids field to markdown files by querying CBDB API'
    )
    parser.add_argument(
        '--data-dir',
        type=Path,
        default=Path('data'),
        help='Root data directory (default: data)'
    )
    parser.add_argument(
        '--pattern',
        type=str,
        help='Glob pattern to filter files (e.g., "诗藏/**/*.md")'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without modifying files'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of files to process (for testing)'
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=0.5,
        help='Delay in seconds between API requests (default: 0.5)'
    )
    parser.add_argument(
        '--report',
        type=Path,
        default=Path('cbdb_ambiguous_cases.json'),
        help='Output file for ambiguous cases report (default: cbdb_ambiguous_cases.json)'
    )

    args = parser.parse_args()

    if not args.data_dir.exists():
        print(f"Error: Data directory not found: {args.data_dir}")
        return 1

    print(f"Searching for markdown files in {args.data_dir}")
    if args.pattern:
        print(f"  Using pattern: {args.pattern}")

    md_files = find_markdown_files(args.data_dir, args.pattern)
    print(f"Found {len(md_files)} markdown files")

    if args.limit:
        md_files = md_files[:args.limit]
        print(f"Processing first {args.limit} files (--limit)")

    if args.dry_run:
        print("\n*** DRY RUN MODE - No files will be modified ***\n")

    # Start timing
    start_time = datetime.now()
    print(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")

    modified_count = 0
    ambiguous_cases = []

    try:
        for i, file_path in enumerate(md_files, 1):
            # Check for shutdown request
            if _shutdown_requested:
                print(f"\n中断处理，已处理 {i-1}/{len(md_files)} 个文件")
                break

            # Calculate progress and ETA
            elapsed = datetime.now() - start_time
            if i > 1:
                avg_time_per_file = elapsed / (i - 1)
                remaining_files = len(md_files) - i
                eta = datetime.now() + (avg_time_per_file * remaining_files)
                progress_percent = (i - 1) / len(md_files) * 100
                print(f"\n[{i}/{len(md_files)} - {progress_percent:.1f}%] 已用时: {str(elapsed).split('.')[0]} | 预计完成: {eta.strftime('%H:%M:%S')}")
            else:
                print(f"\n[{i}/{len(md_files)}]")

            print(f"Processing: {file_path.relative_to(args.data_dir.parent)}")

            if process_markdown_file(file_path, args.dry_run, ambiguous_cases):
                modified_count += 1

            # Add delay between files to avoid overwhelming the API
            if i < len(md_files) and not _shutdown_requested:
                # Use short sleep intervals to allow responsive interrupt handling
                delay_remaining = args.delay
                while delay_remaining > 0 and not _shutdown_requested:
                    sleep_time = min(0.1, delay_remaining)
                    time.sleep(sleep_time)
                    delay_remaining -= sleep_time

    except KeyboardInterrupt:
        print(f"\n\n收到键盘中断，已处理 {i}/{len(md_files)} 个文件")
    except Exception as e:
        print(f"\n\n发生错误: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # Calculate total time
    end_time = datetime.now()
    total_time = end_time - start_time

    print(f"\n{'=' * 60}")
    print(f"完成时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"总耗时: {str(total_time).split('.')[0]}")
    print(f"平均每文件: {str(total_time / len(md_files)).split('.')[0] if md_files else 'N/A'}")
    print(f"{'Would modify' if args.dry_run else 'Modified'} {modified_count}/{len(md_files)} files")
    print(f"缓存统计: {len(_author_cbdb_cache)} 个不同作者名已缓存")
    print(f"{'=' * 60}")

    # Generate report for ambiguous cases
    if ambiguous_cases:
        print(f"\n⚠️  Found {len(ambiguous_cases)} ambiguous file(s) requiring manual review")
        print(f"Writing report to: {args.report}")

        try:
            # Group cases by author_name to avoid duplication
            # Key: author_name, Value: list of cases
            grouped_cases = {}
            for case in ambiguous_cases:
                author_name = case['author_name']
                if author_name not in grouped_cases:
                    grouped_cases[author_name] = {
                        'author_name': author_name,
                        'author_field': case['author_field'],  # Use first occurrence
                        'dynasty': case['dynasty'],
                        'matches': case['matches'],
                        'files': []
                    }
                grouped_cases[author_name]['files'].append(case['file'])

            # Format data for JSON output
            report_data = {
                "generated_at": time.strftime('%Y-%m-%d %H:%M:%S'),
                "total_ambiguous_files": len(ambiguous_cases),
                "unique_authors": len(grouped_cases),
                "cases": []
            }

            for author_name, group in sorted(grouped_cases.items()):
                case_data = {
                    "author_name": author_name,
                    "author_field": group['author_field'],
                    "dynasty": group['dynasty'],
                    "files": sorted(group['files']),
                    "file_count": len(group['files']),
                    "potential_matches": [
                        {
                            "cbdb_id": match['id'],
                            "name": match['name'],
                            "dynasty": match['dynasty'],
                            "year": match['year'],
                            "location": match['location'],
                            "cbdb_url": f"https://cbdb.fas.harvard.edu/cbdbapi/person.php?id={match['id']}"
                        }
                        for match in group['matches']
                    ]
                }
                report_data["cases"].append(case_data)

            with open(args.report, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, ensure_ascii=False, indent=2)

            print(f"✓ Report written successfully")
            print(f"  {len(ambiguous_cases)} files with {len(grouped_cases)} unique authors")

        except Exception as e:
            print(f"Error writing report: {e}")

    else:
        print("\n✓ No ambiguous cases found - all authors matched uniquely or not at all")

    return 0


if __name__ == '__main__':
    exit(main())
