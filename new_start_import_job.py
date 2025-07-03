def start_import_job(task_id):
    """Start the actual import process with batch-oriented architecture."""
    print(f"🚀 [START] Starting batch import job {task_id}")
    
    # Try to get job from both sources
    kuzu_job = get_job_from_kuzu(task_id)
    memory_job = import_jobs.get(task_id)
    
    print(f"📊 [START] Kuzu job found: {bool(kuzu_job)}")
    print(f"💾 [START] Memory job found: {bool(memory_job)}")
    
    job = kuzu_job or memory_job
    if not job:
        print(f"❌ [START] Import job {task_id} not found in start_import_job")
        return

    print(f"✅ [START] Starting import job {task_id} for user {job['user_id']}")
    job['status'] = 'running'
    update_job_in_kuzu(task_id, {'status': 'running'})
    if task_id in import_jobs:
        import_jobs[task_id]['status'] = 'running'

    try:
        csv_file_path = job['csv_file_path']
        mappings = job['field_mappings']
        user_id = job['user_id']
        
        print(f"Processing CSV file: {csv_file_path}")
        print(f"🔍 [MAPPING_DEBUG] Field mappings received: {mappings}")
        print(f"User ID: {user_id} (type: {type(user_id)})")
        
        # BATCH IMPORT FLOW
        print("📋 [BATCH] Starting batch import flow")
        import csv
        
        # PHASE 1: Parse CSV into rows
        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            raw_rows = list(reader)
        print(f"📋 Parsed {len(raw_rows)} rows from CSV")
        
        # PHASE 2: Extract unique identifiers for enrichment
        isbns = set()
        authors = set()
        for row in raw_rows:
            # Normalize ISBN/UID
            isbn_val = normalize_goodreads_value(
                row.get('ISBN') or row.get('ISBN13') or row.get('ISBN/UID', ''), 'isbn'
            )
            if isbn_val:
                isbns.add(isbn_val)
            # Collect author names
            author_name = row.get('Author') or row.get('Authors')
            if author_name:
                authors.add(author_name.strip())
        print(f"🔍 Found {len(isbns)} unique ISBNs, {len(authors)} unique authors for enrichment")
        
        # PHASE 3: Batch metadata enrichment (placeholders)
        book_meta_map = batch_fetch_book_metadata(isbns)
        author_meta_map = batch_fetch_author_metadata(authors)
        
        # PHASE 4: Create custom field definitions before entity creation
        auto_create_custom_fields(mappings, user_id)
        
        # PHASE 5: Create entities and relationships
        from app.simplified_book_service import create_simplified_book_service
        service = create_simplified_book_service()
        
        for row_num, row in enumerate(raw_rows, 1):
            try:
                print(f"📖 [BATCH] Processing row {row_num}/{len(raw_rows)}")
                
                # Build book data and add to user library
                book_data = service.build_book_data_from_row(row, mappings, book_meta_map)
                
                # Extract user-specific data from row
                user_rating = None
                rating_val = row.get('My Rating') or row.get('Star Rating')
                if rating_val:
                    try:
                        user_rating = float(rating_val)
                    except (ValueError, TypeError):
                        pass
                
                personal_notes = row.get('Private Notes') or row.get('Review', '')
                reading_status = job.get('default_reading_status', 'plan_to_read')
                
                # Extract custom metadata
                custom_metadata = {}
                for csv_field, book_field in mappings.items():
                    if book_field.startswith('custom_') and csv_field in row and row[csv_field]:
                        field_name = book_field.replace('custom_global_', '').replace('custom_personal_', '')
                        custom_metadata[field_name] = row[csv_field]
                
                success = service.add_book_to_user_library(
                    book_data,
                    user_id=user_id,
                    reading_status=reading_status,
                    ownership_status='owned',
                    media_type='physical',
                    user_rating=user_rating,
                    personal_notes=personal_notes,
                    custom_metadata=custom_metadata
                )
                
                if success:
                    job['success'] = job.get('success', 0) + 1
                    print(f"✅ [BATCH] Successfully added book from row {row_num}")
                else:
                    job['errors'] = job.get('errors', 0) + 1
                    print(f"❌ [BATCH] Failed to add book from row {row_num}")
                
                job['processed'] = job.get('processed', 0) + 1
                update_job_in_kuzu(task_id, {
                    'processed': job['processed'], 
                    'success': job.get('success', 0), 
                    'errors': job.get('errors', 0),
                    'current_book': book_data.title
                })
                
            except Exception as row_error:
                print(f"❌ [BATCH] Error processing row {row_num}: {row_error}")
                job['errors'] = job.get('errors', 0) + 1
                job['processed'] = job.get('processed', 0) + 1
                
        print(f"📊 Batch import completed. Success: {job.get('success', 0)}, Errors: {job.get('errors', 0)}")
        
        # Mark as completed
        job['status'] = 'completed'
        update_job_in_kuzu(task_id, {
            'status': 'completed',
            'processed': job.get('processed', 0),
            'success': job.get('success', 0),
            'errors': job.get('errors', 0)
        })
        if task_id in import_jobs:
            import_jobs[task_id].update(job)
        job['current_book'] = None
        job['recent_activity'] = job.get('recent_activity', [])
        job['recent_activity'].append(f"Import completed! {job.get('success', 0)} books imported, {job.get('errors', 0)} errors")
        
        # Clean up temp file
        try:
            import os
            os.unlink(csv_file_path)
        except:
            pass
            
    except Exception as e:
        job['status'] = 'failed'
        if 'error_messages' not in job:
            job['error_messages'] = []
        job['error_messages'].append(str(e))
        update_job_in_kuzu(task_id, {'status': 'failed', 'error_messages': job['error_messages']})
        if task_id in import_jobs:
            import_jobs[task_id]['status'] = 'failed'
            if 'error_messages' not in import_jobs[task_id]:
                import_jobs[task_id]['error_messages'] = []
            import_jobs[task_id]['error_messages'].append(str(e))
        print(f"Import job {task_id} failed: {e}")
        import traceback
        traceback.print_exc()
