#!/usr/bin/env python3

import sys
import argparse
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from logic.cli_helper import CLIHelper


def main():
    parser = argparse.ArgumentParser(description='Customer Review System CLI')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Enqueue all reviews command
    enqueue_parser = subparsers.add_parser('enqueue-all-reviews', help='Enqueue all reviews from CSV file')
    enqueue_parser.add_argument('csv_filename', help='CSV filename in data_files folder')
    
    # Queue status command
    status_parser = subparsers.add_parser('queue-status', help='Get current queue status')
    
    # Clear queue command
    clear_parser = subparsers.add_parser('clear-queue', help='Clear all reviews from queue')
    
    # Process single review command
    single_parser = subparsers.add_parser('enqueue-single-review', help='Enqueue a single review')
    single_parser.add_argument('review_id', help='Review ID')
    single_parser.add_argument('date', help='Review date')
    single_parser.add_argument('rating', help='Review rating')
    single_parser.add_argument('text', help='Review text')
    
    # Clear database command
    clear_db_parser = subparsers.add_parser('clear-database', help='Clear all records from database (DANGEROUS)')
    clear_db_parser.add_argument('--password', help='Password for database clearing (required)', required=False)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    cli_helper = CLIHelper()
    
    if args.command == 'enqueue-all-reviews':
        result = cli_helper.enqueue_all_reviews(args.csv_filename)
        print(result['message'])
        if not result['success']:
            sys.exit(1)
    
    elif args.command == 'queue-status':
        result = cli_helper.get_queue_status()
        print(result['message'])
        if result['success']:
            print(f"Queue length: {result['queue_length']}")
            print(f"Redis connected: {result['connected']}")
        else:
            sys.exit(1)
    
    elif args.command == 'clear-queue':
        result = cli_helper.clear_queue()
        print(result['message'])
        if not result['success']:
            sys.exit(1)
    
    elif args.command == 'enqueue-single-review':
        result = cli_helper.process_single_review(
            args.review_id, 
            args.date, 
            args.rating, 
            args.text
        )
        print(result['message'])
        if not result['success']:
            sys.exit(1)
    
    elif args.command == 'clear-database':
        password = args.password if args.password else None
        result = cli_helper.clear_database(password)
        print(result['message'])
        if not result['success']:
            sys.exit(1)


if __name__ == '__main__':
    main()